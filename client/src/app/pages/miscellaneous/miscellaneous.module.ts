import { NgModule } from "@angular/core";
import { NbButtonModule, NbCardModule } from "@nebular/theme";

import { ThemeModule } from "../../@theme/theme.module";
import {
  MiscellaneousRoutingModule,
  routedComponents,
} from "./miscellaneous-routing.module";
import { MiscellaneousComponent } from "./miscellaneous.component";

@NgModule({
  imports: [
    ThemeModule,
    NbCardModule,
    NbButtonModule,
    MiscellaneousRoutingModule,
  ],
  declarations: [MiscellaneousComponent, ...routedComponents],
})
export class MiscellaneousModule {}
