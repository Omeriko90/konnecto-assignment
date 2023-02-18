import { Component, ElementRef, OnInit, ViewChild } from "@angular/core";
import { Gender, ISegment, ISegmentGenderData } from "../../../core/types";
import { ActivatedRoute } from "@angular/router";
import { SegmentService } from "../../../core/services/segment.service";
import { Chart, LegendItem, registerables } from "chart.js";
import ChartDataLabels from "chartjs-plugin-datalabels";

@Component({
  selector: "segment-data",
  templateUrl: "./segment-data.component.html",
  styleUrls: ["./segment-data.component.scss"],
})
export class SegmentDataComponent implements OnInit {
  private _id: string;
  segmentData: ISegment;
  isLoading: boolean;

  @ViewChild("demographicGenderChartCanvas")
  demographicGenderChartCanvas: ElementRef;
  private demographicGenderChart: Chart;
  private genderColors = ["rgba(61, 105, 246, 0.48)", "#D0BCFF"];

  constructor(
    private route: ActivatedRoute,
    private segmentService: SegmentService
  ) {}

  ngOnInit(): void {
    Chart.register(...registerables);

    this._id = this.route.snapshot.params.id;
    this.isLoading = true;

    this.segmentService.getById(this._id).subscribe((response) => {
      this.segmentData = response.data;
      this.getSegmentDrillDownData();
    });
  }

  getSegmentDrillDownData() {
    this.segmentService.getSegmentGenderData(this._id).subscribe((response) => {
      this.isLoading = false;
      this.renderGenderChart(response.data);
    });
  }

  private renderGenderChart(data: ISegmentGenderData[]) {
    const sorted = data.sort((a, b) => (a._id === Gender.Male ? -1 : 1));
    this.demographicGenderChart = new Chart(
      this.demographicGenderChartCanvas.nativeElement,
      {
        plugins: [ChartDataLabels],
        options: {
          responsive: true,
          plugins: {
            datalabels: {
              color: "black",
              font: {
                family: "Roboto",
                size: 16,
                weight: 500,
              },
              formatter: function (value, context) {
                return sorted[context.dataIndex]?._id;
              },
            },
            legend: {
              position: "bottom",
              align: "start",
              labels: {
                pointStyle: "circle",
                usePointStyle: true,
                generateLabels: (chart) => {
                  return chart.data.labels.map((label, index) => {
                    return {
                      text: `${label} - ${sorted[index]?.userPercentage}%`,
                      fillStyle:
                        sorted[index]._id === Gender.Male
                          ? this.genderColors[0]
                          : this.genderColors[1],
                      strokeStyle: "transparent",
                    } as LegendItem;
                  });
                },
              },
            },
          },
        },
        type: "doughnut",
        data: {
          labels: sorted.map((gender) => gender._id),
          datasets: [
            {
              backgroundColor: this.genderColors,
              label: "Gender",
              data: sorted.map((gender) => gender.userPercentage),
            },
          ],
        },
      }
    );
  }
}
