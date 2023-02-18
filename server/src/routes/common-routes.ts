import express, { Router } from "express";
import AdminRoutes from "./admin/admin-routes";
import SegmentRoutes from "./segments/segment-routes";

import { isValid, checkAuth } from "../common/validator";

const router: Router = express.Router();

router.use("/admin", AdminRoutes);
router.use("/segment", checkAuth, isValid, SegmentRoutes);

export default router;
