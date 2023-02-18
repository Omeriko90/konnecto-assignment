import { Injectable } from "@angular/core";
import { HttpClient, HttpParams } from "@angular/common/http";
import { Observable } from "rxjs";
import { API_V1_PREFIX } from "../constants";
import { ISegment, ISegmentGenderData, ISegmentMetaData } from "../types";

@Injectable({
  providedIn: "root",
})
export class SegmentService {
  private readonly SEGMENTS_API = `${API_V1_PREFIX}/segment`;

  constructor(private http: HttpClient) {}

  list(params?: HttpParams): Observable<{
    data: ISegmentMetaData[];
    totalCount: number;
  }> {
    params = params || new HttpParams();
    return this.http.get<{
      data: ISegmentMetaData[];
      totalCount: number;
    }>(`${this.SEGMENTS_API}/`, { params });
  }

  getById(id: string): Observable<{ data: ISegment }> {
    return this.http.get<{
      data: ISegment;
    }>(`${this.SEGMENTS_API}/${id}`);
  }

  getSegmentGenderData(id: string): Observable<{ data: ISegmentGenderData[] }> {
    return this.http.get<{
      data: ISegmentGenderData[];
    }>(`${this.SEGMENTS_API}/gender-data/${id}`);
  }
}
