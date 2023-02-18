import { Request, Response } from "express";
import { handleResponseError } from "../route-handlers/route-error-handler";
import { Collection, InsertOneWriteOpResult } from "mongodb";
import { getDbWrapper } from "../../common/db/mongo-wrapper";
import { IAdmin } from "../../common/types/db-models/admin";

export async function register(req: Request, res: Response): Promise<void> {
  try {
    const newUser: Partial<IAdmin> = {
      created_time: new Date(),
      last_active: new Date(),
      email: req.body.email,
      name: req.body.name,
      password: req.body.password,
    };

    const adminCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("admin");
    const userInsertObj: InsertOneWriteOpResult<IAdmin> =
      await adminCollection.insertOne(newUser);
    res.json({ success: true, token: String(userInsertObj.insertedId) });
  } catch (error) {
    handleResponseError(
      `User register Error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function login(req: Request, res: Response): Promise<void> {
  try {
    // always login successfully for this assignment
    res.json({
      name: "Assignment user",
      token: "123",
    });
    return;

    const adminCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("admin");
    const user: IAdmin = await adminCollection.findOne({
      email: req.body.email,
      password: req.body.password,
    });

    if (user) {
      res.json({
        name: user.name,
        token: String(user._id),
      });
    } else {
      res.statusCode = 403;
      handleResponseError(
        `User login Error: Wrong email or password`,
        "Wrong email or password",
        res
      );
    }
  } catch (error) {
    handleResponseError(
      `User login Error: ${error.message}`,
      error.message,
      res
    );
  }
}
