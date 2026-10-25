import { Logger } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import type { NextFunction, Request, Response } from 'express';

function accessLog(log: Logger) {
  return (req: Request, res: Response, next: NextFunction) => {
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const entry = {
        msg: 'http',
        method: req.method,
        path: req.originalUrl,
        status: res.statusCode,
        duration_ms: Number(process.hrtime.bigint() - start) / 1e6,
      };
      if (req.path === '/healthz') log.debug(entry);
      else if (res.statusCode >= 500) log.error(entry);
      else log.log(entry);
    });
    next();
  };
}

/** Cấu hình chung cho main.ts và e2e test. App phải tạo với `rawBody: true` (webhook ký trên body thô). */
export function configureApp(app: NestExpressApplication) {
  app.disable('x-powered-by');
  app.use(accessLog(new Logger('HTTP')));
  app.enableShutdownHooks();
}
