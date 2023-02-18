import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable } from "rxjs";
import { API_V1_PREFIX } from "../constants";
import { IAdmin } from "../types";

@Injectable({
  providedIn: "root",
})
export class AdminService {
  private readonly ADMIN_API = `${API_V1_PREFIX}/admin`;

  constructor(private http: HttpClient) {}

  login(email: string, password: string): Observable<IAdmin> {
    return this.http.post<IAdmin>(`${this.ADMIN_API}/login`, {
      email,
      password,
    });
  }

  signup(signupForm: {
    name: string;
    email: string;
    password: string;
  }): Observable<{ success: boolean }> {
    return this.http.post<{ success: boolean }>(
      `${this.ADMIN_API}/login`,
      signupForm
    );
  }
}
