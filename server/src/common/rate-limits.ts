import rateLimit from "express-rate-limit";

export const RateLimitsConfig = rateLimit({
  windowMs: 60 * 1000, // 1 minutes
  max: 750, // limit each IP to 100 requests per windowMs
  message: "Too many requests, please try again later.",
});
