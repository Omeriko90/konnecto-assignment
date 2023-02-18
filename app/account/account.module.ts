import { NgModule } from "@angular/core";

import {
  AccountRoutingModule,
  routedComponents,
} from "./account-routing.module";
import { CommonModule } from "@angular/common";
import { MatButtonModule } from "@angular/material/button";
import { ReactiveFormsModule } from "@angular/forms";
import { UIModule } from "../shared/ui/ui.module";

@NgModule({
  declarations: [...routedComponents],
  imports: [
    CommonModule,
    AccountRoutingModule,
    ReactiveFormsModule,
    UIModule,
    MatButtonModule,
  ],
})
export class AccountModule {}
