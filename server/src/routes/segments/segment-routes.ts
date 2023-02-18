import express, { Router } from "express";
import * as SegmentRouteHandler from "./segment-route-handler";
import { isValid, paramMongoId } from "../../common/validator";

const router: Router = express.Router();

router.route("/").get(SegmentRouteHandler.segmentList);

router
  .route("/:id")
  .get(paramMongoId, isValid, SegmentRouteHandler.getSegmentById)
  .patch(paramMongoId, isValid, SegmentRouteHandler.updateSegmentById);

router
  .route("/gender-data/:id")
  .get(paramMongoId, isValid, SegmentRouteHandler.getSegmentGenderData);

export default router;
