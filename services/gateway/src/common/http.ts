import { Logger } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import type { NextFunction, Response } from 'express';
import { randomUUID } from 'node:crypto';
import type { AppConfig } from '../config';
import type { AppRequest } from './request';

const REQUEST_ID_RE = /^[\w.:-]{1,128}$/;

/** Nhận x-request-id từ client/ingress nếu hợp lệ, ngược lại tự sinh; trả lại trong response. */
export function requestId(req: AppRequest, res: Response, next: NextFunction) {
  const incoming = req.header('x-request-id');
  req.id = incoming && REQUEST_ID_RE.test(incoming) ? incoming : randomUUID();
  res.setHeader('x-request-id', req.id);
  next();
}

export function accessLog(log: Logger) {
  return (req: AppRequest, res: Response, next: NextFunction) => {
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const entry = {
        msg: 'http',
        method: req.method,
        path: req.originalUrl,
        status: res.statusCode,
        duration_ms: Number(process.hrtime.bigint() - start) / 1e6,
        request_id: req.id,
        user_id: req.user?.id,
      };
      if (req.path === '/healthz') log.debug(entry);
      else if (res.statusCode >= 500) log.error(entry);
      else log.log(entry);
    });
    next();
  };
}

/** Cấu hình chung cho main.ts và e2e test. */
export function configureApp(app: NestExpressApplication, config: AppConfig) {
  app.set('trust proxy', config.trustProxy);
  app.disable('x-powered-by');
  app.use(requestId, accessLog(new Logger('HTTP')));
  app.enableShutdownHooks();
}
