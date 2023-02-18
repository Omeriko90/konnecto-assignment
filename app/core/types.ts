export interface IAdmin {
  token?: string;
  name: string;
}

export interface ISegment {
  _id: string;
  name: string;
}

export interface ISegmentMetaData extends ISegment {
  userCount: number;
}

export interface ISegmentGenderData {
  _id: Gender;
  userCount: number;
  userPercentage: number;
}

export enum Gender {
  Female = "Female",
  Male = "Male",
}
