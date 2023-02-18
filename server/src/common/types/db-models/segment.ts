import { ObjectId } from "mongodb";
import { Gender } from "./user";

export interface ISegment {
  _id: ObjectId;
  name: string;
}

export interface ISegmentMetaData extends ISegment {
  userCount: number; // the # of users in the segment
  avgIncome: number; // the avg income of the user group
  topGender: number; // the dominant gender of the user group
}

export interface ISegmentGenderData {
  _id: Gender;
  userCount: number;
  userPercentage: number;
}
