import { Component, OnInit, ViewChild } from "@angular/core";
import { HttpParams } from "@angular/common/http";
import { FormControl, FormGroup } from "@angular/forms";
import { ISegmentMetaData } from "../../../core/types";
import { debounceTime, mergeMap } from "rxjs/operators";
import { SegmentService } from "../../../core/services/segment.service";
import { Observable } from "rxjs";
import { MatPaginator, PageEvent } from "@angular/material/paginator";
import { Router } from "@angular/router";

@Component({
  selector: "segment-list",
  templateUrl: "./segment-list.component.html",
  styleUrls: ["./segment-list.component.scss"],
})
export class SegmentListComponent implements OnInit {
  segmentMetaDataList: ISegmentMetaData[] = [];

  totalCount: number;
  searchValue: string;

  columnList: { label: string; key: string }[] = [
    { label: "Name", key: "name" },
    { label: "User Count", key: "userCount" },
    { label: "Avg Income", key: "avgIncome" },
    { label: "Dominant Gender", key: "topGender" },
  ];

  waitingForResponse: boolean;

  form: FormGroup = new FormGroup({
    search: new FormControl(""),
  });

  @ViewChild(MatPaginator) paginator: MatPaginator;

  constructor(private segmentService: SegmentService, private router: Router) {}

  ngOnInit(): void {
    this.form.controls["search"].valueChanges
      .pipe(
        debounceTime(500),
        mergeMap((value) => {
          this.searchValue = value;
          return this.fetchSegments();
        })
      )
      .subscribe((response) => {
        this.handleListResponse(response);
      });

    this.fetchSegments().subscribe(
      (response) => {
        this.handleListResponse(response);
      },
      (error) => {
        console.log(`error fetching list ${error}`);
      }
    );
  }

  private handleListResponse(response: {
    data: ISegmentMetaData[];
    totalCount: number;
  }) {
    this.segmentMetaDataList = response.data;
    this.totalCount = response.totalCount;
  }

  fetchSegments(): Observable<{
    data: ISegmentMetaData[];
    totalCount: number;
  }> {
    let params = new HttpParams();

    if (this.searchValue?.length) {
      params = params.set("q", this.searchValue);
    }
    return this.segmentService.list(params);
  }

  cellClicked(segment: ISegmentMetaData, column: string): void {
    this.router.navigate(["/pages/segments/data", segment._id]);
  }
}
