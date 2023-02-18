import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from "@angular/core";

import { NbMenuModule } from "@nebular/theme";
import { ThemeModule } from "../@theme/theme.module";
import { PagesComponent } from "./pages.component";
import { PagesRoutingModule } from "./pages-routing.module";
import { MatDialogModule } from "@angular/material/dialog";
import { RouterModule } from "@angular/router";
import { MiscellaneousModule } from "./miscellaneous/miscellaneous.module";

@NgModule({
  imports: [
    PagesRoutingModule,
    ThemeModule,
    NbMenuModule,
    MatDialogModule,
    RouterModule,
    MiscellaneousModule,
  ],
  declarations: [PagesComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class PagesModule {}
