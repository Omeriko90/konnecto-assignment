import { Request, Response } from "express";
import { handleResponseError } from "../route-handlers/route-error-handler";
import { Collection, ObjectId } from "mongodb";
import {
  ISegment,
  ISegmentGenderData,
  ISegmentMetaData,
} from "../../common/types/db-models/segment";
import { getDbWrapper } from "../../common/db/mongo-wrapper";
import { Gender } from "../../common/types/db-models/user";

export async function segmentList(req: Request, res: Response): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");

    // todo TASK 1
    // write this function to return { data: ISegmentMetaData[]; totalCount: number };
    // where data is an array of ISegmentMetaData, and totalCount is the # of total segments

    // the "users" collection
    // const userCollection: Collection = await (await getDbWrapper()).getCollection('users');
    // has a "many to many" relationship to the segment collection, check IUser interface or query the raw data.
    // res.json({ success: true, data: ISegmentMetaData[], totalCount });
    const search = req?.query;
    const aggregation: object[] = [];
    if (search?.q) {
      aggregation.push({
        $match: { name: new RegExp(`${search.q as string}`, "i") },
      });
    }
    aggregation.push(
      ...[
        {
          $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "segment_ids",
            as: "users",
          },
        },
        {
          $unwind: {
            path: "$users",
          },
        },
        {
          $addFields: {
            income_level: {
              $cond: [
                {
                  $eq: ["$users.income_type", "yearly"],
                },
                "$users.income_level",
                {
                  $multiply: ["$users.income_level", 12],
                },
              ],
            },
            gender: "$users.gender",
          },
        },
        {
          $group: {
            _id: "$_id",
            name: {
              $first: "$name",
            },
            userCount: {
              $sum: 1,
            },
            totalIncome: {
              $sum: "$income_level",
            },
            female: {
              $sum: {
                $cond: [
                  {
                    $eq: ["$gender", "Female"],
                  },
                  1,
                  0,
                ],
              },
            },
            male: {
              $sum: {
                $cond: [
                  {
                    $eq: ["$gender", "Male"],
                  },
                  1,
                  0,
                ],
              },
            },
          },
        },
        {
          $addFields: {
            avgIncome: {
              $divide: ["$totalIncome", "$userCount"],
            },
            topGender: {
              $cond: [
                {
                  $gt: ["$female", "$male"],
                },
                "$female",
                "$male",
              ],
            },
          },
        },
        {
          $project: {
            _id: 1,
            name: 1,
            userCount: 1,
            avgIncome: 1,
            topGender: 1,
          },
        },
      ]
    );
    const segmentMetaData: ISegmentMetaData[] = await segmentCollection
      .aggregate(aggregation)
      .toArray();

    res.json({
      success: true,
      data: segmentMetaData,
      totalCount: segmentMetaData?.length || 0,
    });
  } catch (error) {
    handleResponseError(
      `Get Segment List Error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");
    const segment: ISegment = await segmentCollection.findOne({
      _id: new ObjectId(req.params.id as string),
    });
    if (!segment) {
      return handleResponseError(
        `Error getSegmentById`,
        `Segment with id ${req.params.id} not found.`,
        res
      );
    }
    res.json({ success: true, data: segment });
  } catch (error) {
    handleResponseError(
      `Get Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function updateSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    // res.json({ success: true });
  } catch (error) {
    handleResponseError(
      `Update Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentGenderData(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");

    // todo TASK 2
    // write this function to return
    // data = [ { _id: "Male", userCount: x1, userPercentage: y1 }, { _id: "Female", userCount: x2, userPercentage: y2} ]

    // the "users" collection
    // const userCollection: Collection = await (await getDbWrapper()).getCollection('users');
    // has a "many to many" relationship to the segment collection, check IUser interface or query the raw data.
    // res.json({ success: true, data: ISegmentGenderData[] });
    const segmentId: string = req.params.id;
    const genderData = await segmentCollection
      .aggregate([
        {
          $match: {
            _id: new ObjectId(segmentId),
          },
        },
        {
          $lookup: {
            from: "users",
            localField: "_id",
            foreignField: "segment_ids",
            as: "users",
          },
        },
        {
          $unwind: {
            path: "$users",
          },
        },
        {
          $addFields: {
            male: {
              $cond: [
                {
                  $eq: ["$users.gender", "Male"],
                },
                1,
                0,
              ],
            },
            female: {
              $cond: [
                {
                  $eq: ["$users.gender", "Female"],
                },
                1,
                0,
              ],
            },
          },
        },
        {
          $group: {
            _id: "$_id",
            maleCount: {
              $sum: "$male",
            },
            femaleCount: {
              $sum: "$female",
            },
            usersCount: {
              $sum: 1,
            },
          },
        },
      ])
      .toArray();
    const maleData: ISegmentGenderData = {
      _id: Gender.Male,
      userCount: genderData?.[0].maleCount,
      userPercentage: genderData?.[0].maleCount / genderData?.[0].usersCount,
    };
    const femaleData: ISegmentGenderData = {
      _id: Gender.Female,
      userCount: genderData?.[0].femaleCount,
      userPercentage: genderData?.[0].femaleCount / genderData?.[0].usersCount,
    };
    res.json({ success: true, data: [maleData, femaleData] });
  } catch (error) {
    handleResponseError(
      `Segment gender data error: ${error.message}`,
      error.message,
      res
    );
  }
}
