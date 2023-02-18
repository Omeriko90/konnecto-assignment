import { ObjectId } from "mongodb";

export interface IAdmin {
  _id: ObjectId;
  created_time: Date;
  last_active: Date;
  email: string;
  name: string;
  password: string;
}
