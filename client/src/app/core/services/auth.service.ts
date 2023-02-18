import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { map } from "rxjs/operators";
import { IAdmin } from "../types";
import { AdminService } from "./admin.service";

@Injectable({ providedIn: "root" })
export class AuthenticationService {
  private currentUserSubject: BehaviorSubject<IAdmin>;
  public currUser: Observable<IAdmin>;

  constructor(private adminService: AdminService) {
    this.currentUserSubject = new BehaviorSubject<IAdmin>(
      JSON.parse(sessionStorage.getItem("user"))
    );
    this.currUser = this.currentUserSubject.asObservable();
  }

  /**
   * Returns the current user
   */
  public currentUser(): IAdmin {
    return this.currentUserSubject.value;
  }

  /**
   * Performs the auth
   * @param email email of user
   * @param password password of user
   */
  login(email: string, password: string): Observable<IAdmin> {
    return this.adminService.login(email, password).pipe(
      map((response: IAdmin) => {
        // login successful if there's a jwt token in the response
        if (response && response.token) {
          // store user details and jwt in storage
          sessionStorage.setItem("user", JSON.stringify(response));
          this.currentUserSubject.next(response);
        }
        return response;
      })
    );
  }

  logout(): void {
    // remove user from local storage to log user out
    sessionStorage.removeItem("user");
    this.currentUserSubject.next(null);
  }
}
