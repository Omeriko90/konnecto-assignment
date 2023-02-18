import express, { Router } from "express";
import * as AdminRouteHandler from "./admin-route-handler";
import { isValid } from "../../common/validator";
import { body } from "express-validator";

const router: Router = express.Router();

const registerValidates = [
  body("name")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty")
    .matches("^[a-zA-Z]+( [a-zA-Z]+)*$")
    .withMessage("input is not [a-zA-Z] with spaces")
    .isLength({ max: 50 })
    .withMessage("input is too long"),

  body("email")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty")
    .isEmail()
    .withMessage("input is not email")
    .normalizeEmail(),

  body("password")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty"),
];

const loginValidates = [
  body("email")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty")
    .isEmail()
    .withMessage("input is not email")
    .normalizeEmail(),

  body("password")
    .exists()
    .withMessage("input does not exist")
    .trim()
    .not()
    .isEmpty()
    .withMessage("input is empty"),
];

router
  .route("/register")
  .post(registerValidates, isValid, AdminRouteHandler.register);
router.route("/login").post(loginValidates, isValid, AdminRouteHandler.login);

export default router;
