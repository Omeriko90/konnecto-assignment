import express from "express";
import { logger } from "./common/logger";
import { connectMongoConnector } from "./common/db/mongo-wrapper";
import MiddlewareBase from "./middleware/middleware-base";

const app = express();
const port: number = Number(process.env.APP_PORT ?? 8000);

// override console with winston.
console.log = (...args) => logger.info.apply(logger, args);
console.error = (...args) => logger.error.apply(logger, args);
console.info = (...args) => logger.info.apply(logger, args);
console.warn = (...args) => logger.warn.apply(logger, args);

connectMongoConnector().then(() => {
  app.use("/api/v1", MiddlewareBase.configuration);

  app
    .listen(port, () => console.log(`server is listening on ${port}`))
    .on("error", (err) => console.error(err?.message || err));
});
