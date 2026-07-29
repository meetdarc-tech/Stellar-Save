import { Router } from 'express';
import { format as fastCsvFormat } from 'fast-csv';

import { RecommendationEngine } from '../recommendation';
import { EmailService } from '../email_service';
import { ExportService } from '../export_service';
import { parseOffsetParams, parseCursorParams, paginate, paginateArray, paginateCursorArray } from '../lib/pagination';
import { BackupService, S3HttpClient } from '../backup_service';
import { BackupScheduler } from '../backup_scheduler';
import { RecoveryService } from '../recovery_service';
import { BackupMonitor } from '../backup_monitor';
import { BackupRestoreDrill } from '../backup_restore_drill';
import { ContractEventIndexer } from '../contract_event_indexer';
import { AnalyticsService } from '../analytics_service';
import { FeedbackService } from '../feedback_service';
import { createAnalyticsMiddlewareStack, createAnalyticsCacheMiddleware } from '../analytics_middleware';
import { Group, UserInteraction, UserPreference } from '../models';
import { createNotificationRouter } from './notifications';
import { createSseRouter } from './sse';
import { createInsuranceRouter } from './insurance';
import { createGovernanceRouter } from './governance';
import { adminAuthMiddleware } from '../auth_middleware';
import { apiKeyService } from '../api_key_service';
import { apiKeyAuthMiddleware, recordApiUsage } from '../api_key_rate_limiter';
import { AdminService } from '../admin_service';
import { readinessCheckCache } from '../redis';

// ── Shared service instances (passed in from app) ────────────────────────────
export interface V1Services {
  engine: RecommendationEngine;
  exportService: ExportService;
  backupService: BackupService;
  backupScheduler: BackupScheduler;
  recoveryService: RecoveryService;
  backupMonitor: BackupMonitor;
  backupRestoreDrill: BackupRestoreDrill;
  eventIndexer: ContractEventIndexer;
  analyticsService: AnalyticsService;
  feedbackService: FeedbackService;
}

export function createV1Router(services: V1Services): Router {
  const router = Router();
  const {
    engine,
    exportService,
    backupService,
    backupScheduler,
    recoveryService,
    backupMonitor,
    backupRestoreDrill,
    eventIndexer,
    analyticsService,
    feedbackService,
  } = services;

  // Setup analytics middleware
  const analyticsMiddleware = createAnalyticsMiddlewareStack();
  // 5-minute cache specifically for the landing page stats endpoint
  const statsGroupsCache = createAnalyticsCacheMiddleware(300);

  // ── Landing Page Stats ────────────────────────────────────────────────────
  // GET /stats/groups — platform-wide group statistics for the landing page.
  // Aggregates from the indexed ContractEvent database; cached 5 min in Redis.
  router.get(
    '/stats/groups',
    analyticsMiddleware.readRateLimit,
    statsGroupsCache,
    async (_req, res) => {
      try {
        const stats = await analyticsService.getGroupsOverviewStats();
        res.json(stats);
      } catch (error) {
        console.error('Error fetching groups overview stats:', error);
        res.status(500).json({ error: 'Failed to fetch group statistics' });
      }
    }
  );

  // Notifications (web push subscriptions, preferences, templates)
  router.use('/notifications', createNotificationRouter());

  // SSE event stream (Issue #1011)
  router.use('/events', createSseRouter(eventIndexer));

  // Insurance pool (Issue #1012)
  router.use('/groups/:groupId/insurance', createInsuranceRouter());

  // Governance proposals (Issue #1013)
  router.use('/governance', createGovernanceRouter());

  // Search
  router.get('/search', async (req, res) => {
    const { q } = req.query;
    if (!q) return res.status(400).json({ error: 'Query parameter q is required' });
    try {
      const { SearchService } = await import('../search');
      const searchService = new SearchService();
      res.json(await searchService.globalSearch(q as string));
    } catch {
      res.status(500).json({ error: 'Search failed' });
    }
  });

  router.get('/search/autocomplete', async (req, res) => {
    const { q } = req.query;
    if (!q) return res.status(400).json({ error: 'Query parameter q is required' });
    try {
      const { SearchService } = await import('../search');
      const searchService = new SearchService();
      res.json(await searchService.autocomplete(q as string));
    } catch {
      res.status(500).json({ error: 'Autocomplete failed' });
    }
  });

  // Preferences
  router.post('/preferences', (req, res) => {
    const pref: UserPreference = req.body;
    if (!pref.userId) return res.status(400).json({ error: 'userId is required' });
    engine.setPreference(pref);
    res.status(200).json({ message: 'Preferences updated' });
  });

  // Recommendations
  router.get('/recommendations/:userId', (req, res) => {
    const { userId } = req.params;
    const recommendations = engine.getRecommendations(userId, 'collaborative');
    res.json({ userId, algorithm: 'collaborative', recommendations });
  });

  // Health
  router.get('/health', (req, res) => {
    res.json({
      status: 'ok',
      version: 'v1',
    });
  });

  // Ready
  router.get('/ready', async (req, res) => {
    const requestStart = Date.now();

    const [database, horizon, cache] = await Promise.all([
      eventIndexer.readinessCheckDatabase(),
      eventIndexer.readinessCheckHorizon(),
      readinessCheckCache(),
    ]);

    const responseTimeMs = Date.now() - requestStart;
    const up = database.up && horizon.up && cache.up;

    res.status(up ? 200 : 503).json({
      status: up ? 'ready' : 'not_ready',
      version: 'v1',
      responseTimeMs,
      dependencies: {
        database,
        horizon,
        cache,
      },
    });
  });

  // Export
  router.post('/export', async (req, res) => {
    const { userId, email, format } = req.body;
    if (!userId || !email || !format)
      return res.status(400).json({ error: 'userId, email, and format are required' });
    if (format !== 'CSV' && format !== 'JSON')
      return res.status(400).json({ error: 'Invalid format. Use CSV or JSON' });
    const jobId = await exportService.createJob(userId, email, format);
    res.status(202).json({ jobId, message: 'Export job created' });
  });

  router.get('/export/:jobId', (req, res) => {
    const job = exportService.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  });

  router.get('/export/:jobId/download', (req, res) => {
    const job = exportService.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    if (job.status !== 'completed')
      return res.status(400).json({ error: 'Job is not completed yet' });
    res.json({ url: job.fileUrl });
  });

  // Backup
  router.post('/backup', async (req, res) => {
    const { type } = req.body;
    if (type !== 'full' && type !== 'incremental')
      return res.status(400).json({ error: 'type must be "full" or "incremental"' });
    const job = await backupScheduler.triggerManual(type);
    res.status(202).json(job);
  });

  router.get('/backup', (_req, res) => res.json(backupService.listJobs()));

  router.get('/backup/alerts', (req, res) => {
    const unacknowledgedOnly = req.query.unacknowledgedOnly === 'true';
    res.json(backupMonitor.getAlerts(unacknowledgedOnly));
  });

  router.post('/backup/alerts/:alertId/acknowledge', (req, res) => {
    const ok = backupMonitor.acknowledge(req.params.alertId);
    if (!ok) return res.status(404).json({ error: 'Alert not found' });
    res.json({ acknowledged: true });
  });

  router.get('/backup/:jobId', (req, res) => {
    const job = backupService.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Backup job not found' });
    res.json(job);
  });

  router.post('/backup/restore', async (req, res) => {
    try {
      const result = req.body.jobId
        ? await recoveryService.restore(req.body.jobId)
        : await recoveryService.restoreLatest();
      res.json(result);
    } catch (err: unknown) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.get('/backup/drills', (_req, res) => res.json(backupRestoreDrill.listRuns()));

  router.get('/backup/drills/alerts', (req, res) => {
    const unacknowledgedOnly = req.query.unacknowledgedOnly === 'true';
    res.json(backupRestoreDrill.listAlerts(unacknowledgedOnly));
  });

  router.post('/backup/drills/alerts/:alertId/acknowledge', (req, res) => {
    const ok = backupRestoreDrill.acknowledge(req.params.alertId);
    if (!ok) return res.status(404).json({ error: 'Alert not found' });
    res.json({ acknowledged: true });
  });

  router.post('/backup/drills/run', async (_req, res) => {
    const run = await backupRestoreDrill.runDrill();
    res.status(202).json(run);
  });

  // Contract Event Indexer Endpoints
  router.get('/events', async (req, res) => {
    try {
      const { contractId, eventType, startLedger, endLedger, startTime, endTime } =
        req.query;

      const pageParams = parseOffsetParams(req.query);

      const options: any = {};
      if (contractId) options.contractId = contractId as string;
      if (eventType) options.eventType = eventType as string;
      if (startLedger) options.startLedger = parseInt(startLedger as string);
      if (endLedger) options.endLedger = parseInt(endLedger as string);
      if (startTime) options.startTime = new Date(startTime as string);
      if (endTime) options.endTime = new Date(endTime as string);
      options.limit = pageParams.limit;
      options.offset = pageParams.offset;

      const result = await eventIndexer.getEvents(options);

      // result may be an array or an object — normalise to PaginatedResult
      const items: any[] = Array.isArray(result) ? result : (result as any).events ?? [];
      const total: number = Array.isArray(result)
        ? items.length
        : (result as any).total ?? items.length;
      res.json(paginate(items, total, pageParams));
    } catch (error) {
      console.error('Error fetching events:', error);
      res.status(500).json({ error: 'Failed to fetch events' });
    }
  });

  router.get('/events/stats', async (req, res) => {
    try {
      const { contractId } = req.query;
      // Get basic stats about events
      const totalEvents = await (eventIndexer as any).prisma.contractEvent.count({
        where: contractId ? { contractId: contractId as string } : {},
      });

      const eventTypes = await (eventIndexer as any).prisma.contractEvent.groupBy({
        by: ['eventType'],
        where: contractId ? { contractId: contractId as string } : {},
        _count: { eventType: true },
      });

      res.json({
        totalEvents,
        eventTypeBreakdown: eventTypes.map((type: any) => ({
          type: type.eventType,
          count: type._count.eventType,
        })),
      });
    } catch (error) {
      console.error('Error fetching event stats:', error);
      res.status(500).json({ error: 'Failed to fetch event stats' });
    }
  });

  // ── Analytics Endpoints (Issue #558) ────────────────────────────

  // Get platform statistics for a specific date
  router.get(
    '/analytics/platform',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { date } = req.query;
        const targetDate = date ? new Date(date as string) : new Date();
        const stats = await analyticsService.getPlatformStats(targetDate);

        if (!stats) {
          return res.status(404).json({ error: 'No analytics data available for this date' });
        }

        res.json(stats);
      } catch (error) {
        console.error('Error fetching platform stats:', error);
        res.status(500).json({ error: 'Failed to fetch platform statistics' });
      }
    }
  );

  // Get platform trends over a date range
  router.get(
    '/analytics/platform/trends',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { startDate, endDate } = req.query;

        if (!startDate || !endDate) {
          return res.status(400).json({ error: 'startDate and endDate are required' });
        }

        const pageParams = parseOffsetParams(req.query, { limit: 30 });

        const trends = await analyticsService.getPlatformTrends(
          new Date(startDate as string),
          new Date(endDate as string),
          pageParams
        );

        res.json({
          startDate,
          endDate,
          ...paginate(trends, trends.length, pageParams),
        });
      } catch (error) {
        console.error('Error fetching platform trends:', error);
        res.status(500).json({ error: 'Failed to fetch platform trends' });
      }
    }
  );

  // Get user-specific analytics
  router.get(
    '/analytics/users/:userId',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { userId } = req.params;
        const { date } = req.query;
        const targetDate = date ? new Date(date as string) : new Date();

        const stats = await analyticsService.getUserStats(userId, targetDate);

        if (!stats) {
          return res.status(404).json({ error: 'No analytics data available for this user' });
        }

        res.json(stats);
      } catch (error) {
        console.error('Error fetching user stats:', error);
        res.status(500).json({ error: 'Failed to fetch user statistics' });
      }
    }
  );

  // Get group-specific analytics
  router.get(
    '/analytics/groups/:groupId',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { groupId } = req.params;
        const { date } = req.query;
        const targetDate = date ? new Date(date as string) : new Date();

        const stats = await analyticsService.getGroupStats(groupId, targetDate);

        if (!stats) {
          return res.status(404).json({ error: 'No analytics data available for this group' });
        }

        res.json(stats);
      } catch (error) {
        console.error('Error fetching group stats:', error);
        res.status(500).json({ error: 'Failed to fetch group statistics' });
      }
    }
  );

  // Get analytics events statistics
  router.get(
    '/analytics/events',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { startDate, endDate } = req.query;
        const pageParams = parseOffsetParams(req.query, { limit: 20 });

        const eventStats = await analyticsService.getEventStats({
          startDate: startDate ? new Date(startDate as string) : undefined,
          endDate: endDate ? new Date(endDate as string) : undefined,
          ...pageParams,
        });

        res.json(paginate(eventStats, eventStats.length, pageParams));
      } catch (error) {
        console.error('Error fetching event stats:', error);
        res.status(500).json({ error: 'Failed to fetch event statistics' });
      }
    }
  );

  // Record an analytics event
  router.post('/analytics/events', analyticsMiddleware.writeRateLimit, async (req, res) => {
    try {
      const { eventType, eventName, userId, groupId, eventData, sessionId } = req.body;

      if (!eventType || !eventName) {
        return res.status(400).json({
          error: 'eventType and eventName are required',
        });
      }

      await analyticsService.recordEvent(
        eventType,
        eventName,
        userId,
        groupId,
        eventData,
        sessionId
      );

      res.status(201).json({ message: 'Event recorded successfully' });
    } catch (error) {
      console.error('Error recording event:', error);
      res.status(500).json({ error: 'Failed to record event' });
    }
  });

  // Generate an analytics report
  router.post('/analytics/reports', analyticsMiddleware.writeRateLimit, async (req, res) => {
    try {
      const { reportType, reportName, startDate, endDate, generatedBy } = req.body;

      if (!reportType || !reportName || !startDate || !endDate) {
        return res.status(400).json({
          error: 'reportType, reportName, startDate, and endDate are required',
        });
      }

      const report = await analyticsService.generateReport(
        reportType,
        reportName,
        new Date(startDate),
        new Date(endDate),
        generatedBy
      );

      res.status(201).json(report);
    } catch (error) {
      console.error('Error generating report:', error);
      res.status(500).json({ error: 'Failed to generate report' });
    }
  });

  // Get analytics reports
  router.get(
    '/analytics/reports',
    analyticsMiddleware.readRateLimit,
    analyticsMiddleware.cache,
    async (req, res) => {
      try {
        const { reportType } = req.query;
        const pageParams = parseOffsetParams(req.query, { limit: 20 });

        const reports = await analyticsService.getReports(reportType as string, pageParams);

        res.json(paginate(reports, reports.length, pageParams));
      } catch (error) {
        console.error('Error fetching reports:', error);
        res.status(500).json({ error: 'Failed to fetch reports' });
      }
    }
  );

  // Get cache statistics
  router.get('/analytics/cache/stats', analyticsMiddleware.readRateLimit, async (req, res) => {
    try {
      const stats = await analyticsService.getCacheStats();
      res.json(stats);
    } catch (error) {
      console.error('Error fetching cache stats:', error);
      res.status(500).json({ error: 'Failed to fetch cache statistics' });
    }
  });

  // Clear analytics cache
  router.post('/analytics/cache/clear', analyticsMiddleware.writeRateLimit, async (req, res) => {
    try {
      const { pattern } = req.body;
      const cachePattern = pattern || '*';

      await analyticsService.clearCache(cachePattern);
      res.json({ message: 'Cache cleared successfully' });
    } catch (error) {
      console.error('Error clearing cache:', error);
      res.status(500).json({ error: 'Failed to clear cache' });
    }
  });

  // Members export (CSV streaming) for tax/accounting
  // GET /api/members/:address/export.csv
  router.get('/members/:address/export.csv', async (req, res) => {
    const { address } = req.params;

    // Delay loading mock data to keep startup fast
    const { mockTransactions, mockGroups } = await import('../mock_data');

    const transactions = mockTransactions
      .filter((t) => t.memberAddress === address)
      .sort((a, b) => a.timestamp - b.timestamp);

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${encodeURIComponent(address)}-contributions-payouts.csv"`
    );

    // Stream rows without buffering full dataset in memory.
    const csvStream = fastCsvFormat({
      headers: ['date', 'group_id', 'type', 'amount', 'transaction_hash'],
    });

    csvStream.on('error', (err: any) => {
      console.error('CSV stream error:', err);
      if (!res.headersSent) res.status(500).end();
    });

    csvStream.pipe(res);

    for (const t of transactions) {
      csvStream.write({
        date: new Date(t.timestamp).toISOString(),
        group_id: t.groupId,
        type: t.type,
        amount: t.amount,
        transaction_hash: t.stellarTxHash,
      });
    }

    csvStream.end();
  });

  // ── Admin Dashboard Endpoints ────────────────────────────────────────────
  const adminService = new AdminService();

  // GET /api/v1/admin/stats — Platform statistics for dashboard
  router.get('/admin/stats', adminAuthMiddleware, async (_req, res) => {
    try {
      const stats = adminService.getPlatformStats();
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch platform stats' });
    }
  });

  // GET /api/v1/admin/users — List all users for moderation
  router.get('/admin/users', adminAuthMiddleware, async (_req, res) => {
    try {
      const users = adminService.getUsers();
      res.json({ users });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch users' });
    }
  });

  // PATCH /api/v1/admin/users/:id — Update user (flag/unflag, etc)
  router.patch('/admin/users/:id', adminAuthMiddleware, async (req: any, res) => {
    try {
      const { id } = req.params;
      const { updates, adminId } = req.body;
      if (!updates) return res.status(400).json({ error: 'updates is required' });
      if (!adminId) return res.status(400).json({ error: 'adminId is required' });

      const updated = adminService.updateUser(id, updates, adminId);
      if (!updated) return res.status(404).json({ error: 'User not found' });
      res.json(updated);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update user' });
    }
  });

  // DELETE /api/v1/admin/users/:id — Delete user
  router.delete('/admin/users/:id', adminAuthMiddleware, async (req: any, res) => {
    try {
      const { id } = req.params;
      const { adminId } = req.body;
      if (!adminId) return res.status(400).json({ error: 'adminId is required' });

      const deleted = adminService.deleteUser(id, adminId);
      if (!deleted) return res.status(404).json({ error: 'User not found' });
      res.json({ message: 'User deleted successfully' });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete user' });
    }
  });

  // GET /api/v1/admin/groups — List all groups for moderation
  router.get('/admin/groups', adminAuthMiddleware, async (_req, res) => {
    try {
      // Get groups from contract event indexer or mock data
      const { mockGroups } = await import('../mock_data');
      res.json({ groups: mockGroups });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch groups' });
    }
  });

  // POST /api/v1/admin/groups/:id/flag — Flag/unflag a group for review
  router.post('/admin/groups/:id/flag', adminAuthMiddleware, async (req: any, res) => {
    try {
      const { id } = req.params;
      const { flagged, adminId } = req.body;
      if (typeof flagged !== 'boolean') return res.status(400).json({ error: 'flagged must be boolean' });
      if (!adminId) return res.status(400).json({ error: 'adminId is required' });

      // For now, return the flagged group (in production, persist this)
      const { mockGroups } = await import('../mock_data');
      const group = mockGroups.find((g: any) => g.id === id);
      if (!group) return res.status(404).json({ error: 'Group not found' });

      // Log the action
      adminService.logAction(adminId, 'FLAG_GROUP', id, 'Group', { flagged });

      res.json({ ...group, flagged });
    } catch (error) {
      res.status(500).json({ error: 'Failed to flag group' });
    }
  });

  // GET /api/v1/admin/audit-logs — Fetch audit logs (redirects to /api/admin/audit-log)
  router.get('/admin/audit-logs', adminAuthMiddleware, async (_req, res) => {
    try {
      const logs = adminService.getAuditLogs();
      res.json({ logs });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch audit logs' });
    }
  });

  // ── API Key Management (Issue #1030) ──────────────────────────────────────

  // POST /api/v1/api-keys — Create a new API key
  router.post('/api-keys', async (req: any, res: any) => {
    try {
      const { userId } = req.body;
      if (!userId) return res.status(400).json({ error: 'userId is required' });

      const { key, info } = await apiKeyService.generateKey(userId, req.body.name || 'API Key', req.body.tier || 'free');
      res.status(201).json({ key, info: { ...info, keyPrefix: info.keyPrefix } });
    } catch (error) {
      res.status(500).json({ error: 'Failed to generate API key' });
    }
  });

  // GET /api/v1/api-keys — List API keys for authenticated user
  router.get('/api-keys', apiKeyAuthMiddleware, async (req: any, res: any) => {
    try {
      const keys = await apiKeyService.getKeysForUser(req.apiKey.userId);
      res.json({ keys });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch API keys' });
    }
  });

  // DELETE /api/v1/api-keys/:keyId — Revoke an API key
  router.delete('/api-keys/:keyId', apiKeyAuthMiddleware, async (req: any, res: any) => {
    try {
      const { keyId } = req.params;
      await apiKeyService.revokeKey(keyId);
      res.json({ message: 'API key revoked' });
    } catch (error) {
      res.status(500).json({ error: 'Failed to revoke API key' });
    }
  });

  // GET /api/v1/api-keys/:keyId/usage — Get usage stats for a key
  router.get('/api-keys/:keyId/usage', apiKeyAuthMiddleware, async (req: any, res: any) => {
    try {
      const { keyId } = req.params;
      const stats = await apiKeyService.getUsageStats(keyId, parseInt(req.query.hours as string) || 24);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch usage stats' });
    }
  });

  // ── Public API Endpoints (Issue #1030) ────────────────────────────────────

  // GET /api/v1/public/groups — Public list of groups
  router.get('/public/groups', apiKeyAuthMiddleware, async (req: any, res: any) => {
    try {
      const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
      const offset = parseInt(req.query.offset as string) || 0;

      const groups = await (eventIndexer as any).prisma.contractEvent.findMany({
        where: { eventType: 'GroupCreated' },
        orderBy: { timestamp: 'desc' },
        take: limit,
        skip: offset,
      });

      await recordApiUsage(req, res);
      res.json({ count: groups.length, limit, offset, groups });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch groups' });
    }
  });

  // GET /api/v1/public/stats — Public platform statistics
  router.get('/public/stats', apiKeyAuthMiddleware, async (req: any, res: any) => {
    try {
      const stats = await analyticsService.getGroupsOverviewStats();
      await recordApiUsage(req, res);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch statistics' });
    }
  });

  return router;
}
