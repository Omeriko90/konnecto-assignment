import { createLogger, format, transports } from "winston";
import { LoggerOptions, requestWhitelist } from "express-winston";

requestWhitelist.push("body");

const logger = createLogger({
  level: "info",
  exitOnError: false,
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.simple()
  ),
  transports: [new transports.Console()],
  handleExceptions: true,
  defaultMeta: true,
});

const loggerOptions: LoggerOptions = {
  level: "info",
  transports: [new transports.Console()],
  format: format.combine(
    format.timestamp(),
    format.errors({ stack: true }),
    format.simple()
  ),
  meta: true, // optional: control whether you want to log the meta data about the request (default to true)
  expressFormat: true, // Use the default Express/morgan request formatting. Enabling this will override any msg if true. Will only output colors with colorize set to true
  colorize: false, // Color the text and status code, using the Express/morgan color palette (text: gray, status: default green, 3XX cyan, 4XX yellow, 5XX red).
  ignoreRoute: (req, res) => false, // optional: allows to skip some log messages based on request and/or response
};

// @ts-ignore
logger.time = (label: string | undefined) => console.time(label);
// @ts-ignore
logger.timeEnd = (label: string | undefined) => console.timeEnd(label);

export { logger, loggerOptions };
