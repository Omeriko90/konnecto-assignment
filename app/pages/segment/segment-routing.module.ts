import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";
import { SegmentComponent } from "./segment.component";
import { SegmentListComponent } from "./segment-list/segment-list.component";
import { SegmentDataComponent } from "./segment-data/segment-data.component";

const routes: Routes = [
  {
    path: "",
    component: SegmentComponent,
    children: [
      { path: "list", component: SegmentListComponent },
      { path: "data/:id", component: SegmentDataComponent },
    ],
  },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class SegmentRoutingModule {}

export const routedComponents = [
  SegmentComponent,
  SegmentDataComponent,
  SegmentListComponent,
];
