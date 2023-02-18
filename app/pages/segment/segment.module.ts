import { NgModule } from "@angular/core";
import {
  SegmentRoutingModule,
  routedComponents,
} from "./segment-routing.module";

import { CommonModule } from "@angular/common";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { ReactiveFormsModule } from "@angular/forms";
import { NbCardModule, NbTabsetModule } from "@nebular/theme";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatInputModule } from "@angular/material/input";
import { MatPaginatorModule } from "@angular/material/paginator";
import { MatProgressSpinnerModule } from "@angular/material/progress-spinner";

@NgModule({
  declarations: [...routedComponents],
  imports: [
    CommonModule,
    SegmentRoutingModule,
    DragDropModule,
    NbTabsetModule,
    NbCardModule,
    MatFormFieldModule,
    MatIconModule,
    MatInputModule,
    MatPaginatorModule,
    MatProgressSpinnerModule,
    ReactiveFormsModule,
  ],
})
export class SegmentModule {}
