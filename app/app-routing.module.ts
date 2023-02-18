import { RouterModule, Routes } from "@angular/router";
import { NgModule } from "@angular/core";
import { AuthGuard } from "./core/guards/auth.guard";

export const routes: Routes = [
  {
    path: "account",
    loadChildren: () =>
      import("./account/account.module").then((m) => m.AccountModule),
  },
  {
    path: "pages",
    loadChildren: () =>
      import("./pages/pages.module").then((m) => m.PagesModule),
    canActivate: [AuthGuard],
    runGuardsAndResolvers: "always",
  },
  { path: "", redirectTo: "pages", pathMatch: "full" },
  { path: "**", redirectTo: "pages" },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}
