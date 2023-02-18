import { NextFunction, Request, Response } from "express";
import {
  body,
  header,
  param,
  query,
  validationResult,
} from "express-validator";

export const checkAuth = [
  /* Konnecto Auth Header */
  header("authorization")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty"),
];

export const queryMongoId = [
  query("id")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty")
    .isMongoId()
    .withMessage("Input not object id"),
];

export const paramMongoId = [
  param("id")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty")
    .isMongoId()
    .withMessage("Input not object id"),
];

export function isValid(
  req: Request,
  res: Response,
  next: NextFunction
): Response<{}> {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    console.error(
      `Validation Error in route: ${
        req.originalUrl
      }, errors are: \n${JSON.stringify(errors.mapped())}`
    );
    /* 422 Unprocessable Entity */
    const messages = errors.array().map((err) => err.msg);
    return res.status(422).json({ msg: messages[0] });
  } else {
    next();
  }
}
