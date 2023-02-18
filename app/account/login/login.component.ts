import { AfterViewInit, Component, OnInit } from "@angular/core";
import {
  AbstractControl,
  FormBuilder,
  FormGroup,
  Validators,
} from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { first } from "rxjs/operators";
import { AuthenticationService } from "../../core/services/auth.service";

@Component({
  selector: "app-login",
  templateUrl: "./login.component.html",
  styleUrls: ["./login.component.scss"],
})
export class LoginComponent implements OnInit, AfterViewInit {
  loginForm: FormGroup;
  submitted = false;
  returnUrl: string;
  error = "";
  loading = false;
  year = 2020;

  constructor(
    private formBuilder: FormBuilder,
    private route: ActivatedRoute,
    private router: Router,
    private auth: AuthenticationService
  ) {}

  ngOnInit(): void {
    this.loginForm = this.formBuilder.group({
      email: ["", [Validators.required, Validators.email]],
      password: ["", Validators.required],
    });

    // reset login status
    this.auth.logout();
    this.year = new Date().getFullYear();

    // get return url from route parameters or default to '/'
    this.returnUrl = this.route.snapshot.queryParams.returnUrl || "/";
  }

  ngAfterViewInit(): void {
    document.body.classList.add("authentication-bg");
  }

  // convenience getter for easy access to form fields
  get f(): { [key: string]: AbstractControl } {
    return this.loginForm.controls;
  }

  /**
   * On submit form
   */
  onSubmit(): void {
    this.submitted = true;

    // stop here if form is invalid
    if (this.loginForm.invalid) {
      return;
    }

    this.loading = true;

    setTimeout(() => {
      this.auth
        .login(this.f.email.value, this.f.password.value)
        .pipe(first())
        .subscribe(
          (data) => {
            this.router.navigate([this.returnUrl]);
          },
          (error) => {
            this.error = "Wrong Email or Password";
            this.loading = false;
          }
        );
    }, 3000);
  }
}
