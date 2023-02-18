import express, { Express } from "express";
import helmet from "helmet";
import { RateLimitsConfig } from "../common/rate-limits";
import { loggerOptions } from "../common/logger";
import * as expressWinston from "express-winston";
import CommonRoutes from "../routes/common-routes";

export default class MiddlewareBase {
  static get configuration(): Express {
    const app = express();
    app.enable("trust proxy");

    app.use(RateLimitsConfig);

    app.use(helmet());
    app.use(expressWinston.logger(loggerOptions));

    app.use(express.json({ limit: "5mb" }));
    app.use(express.urlencoded({ limit: "5mb", extended: true }));

    app.use(CommonRoutes);
    return app;
  }
}

Object.seal(MiddlewareBase);
