import { Response } from "express";

export function handleResponseError(
  error: string,
  msg: string,
  res: Response
): void {
  const result = { success: false, msg, error: "" };

  switch (res.statusCode) {
    case 418:
      result.error = "expired";
      console.warn(error);
      break;
    case 401:
    case 403:
      console.warn(error);
      result.error = "unauthorized";
      break;
    default:
      console.error(error);
      res.statusCode = 500;
      result.error = error;
  }
  res.json(result);
}
