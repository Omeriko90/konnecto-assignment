import { Component, OnInit } from "@angular/core";
import {
  AbstractControl,
  FormBuilder,
  FormGroup,
  Validators,
} from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { AdminService } from "../../core/services/admin.service";

@Component({
  selector: "app-signup",
  templateUrl: "./signup.component.html",
  styleUrls: ["./signup.component.scss"],
})
export class SignupComponent implements OnInit {
  signupForm: FormGroup;
  error = "";
  submitted = false;
  loading = false;
  year: number = new Date().getFullYear();

  constructor(
    private adminService: AdminService,
    private formBuilder: FormBuilder,
    private route: ActivatedRoute,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.signupForm = this.formBuilder.group({
      name: ["", Validators.required],
      email: ["", [Validators.required, Validators.email]],
      password: ["", Validators.required],
    });
  }

  // convenience getter for easy access to form fields
  get f(): { [key: string]: AbstractControl } {
    return this.signupForm.controls;
  }

  /**
   * On submit form
   */
  onSubmit(): void {
    this.submitted = true;

    // stop here if form is invalid
    if (this.signupForm.invalid) {
      return;
    }

    this.loading = true;
    setTimeout(() => {
      return this.adminService.signup(this.signupForm.value).subscribe(
        (data) => {
          this.router.navigate(["/account/login"]);
        },
        (error) => {
          this.loading = false;
          this.error = "An error has occured.";
        }
      );
    }, 3000);
  }
}
