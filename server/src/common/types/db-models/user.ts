import { ObjectId } from "mongodb";

export interface IUser {
  _id: ObjectId;
  segment_ids: ObjectId[];
  age: number;
  gender?: Gender; //optional
  income_level: string;
  income_type: IncomeType;
}

export enum IncomeType {
  Monthly,
  Yearly,
}

export enum Gender {
  Female = "Female",
  Male = "Male",
  Other = "Other",
}
