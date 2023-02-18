import { Injectable } from "@angular/core";
import {
  Router,
  CanActivate,
  ActivatedRouteSnapshot,
  RouterStateSnapshot,
} from "@angular/router";
import { AuthenticationService } from "../services/auth.service";
import { IAdmin } from "../types";

@Injectable({ providedIn: "root" })
export class AuthGuard implements CanActivate {
  constructor(
    private authenticationService: AuthenticationService,
    private router: Router
  ) {}

  canActivate(
    route: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ): boolean {
    const currentUser: IAdmin = this.authenticationService.currentUser();
    if (currentUser) {
      return true;
    } else {
      this.router.navigate(["/account/login"], {
        queryParams: { returnUrl: state.url },
      });
      return false;
    }
  }
}
