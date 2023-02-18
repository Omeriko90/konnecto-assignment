import { Component } from "@angular/core";

@Component({
  selector: "ngx-footer",
  styleUrls: ["./footer.component.scss"],
  template: `
    <span class="created-by">
      Created with ♥ by
      <b><a href="https://www.konnecto.com/" target="_blank">Konnecto</a></b>
      2021
    </span>
    <div class="socials">
      <a
        href="https://github.com/konnecto/"
        target="_blank"
        class="ion ion-social-github"
      ></a>
      <a
        href="https://www.facebook.com/konnecto.io/"
        target="_blank"
        class="ion ion-social-facebook"
      ></a>
      <a
        href="https://twitter.com/konnecto_?lang=en"
        target="_blank"
        class="ion ion-social-twitter"
      ></a>
      <a
        href="https://www.linkedin.com/company/konnectoconsumerintelligence"
        target="_blank"
        class="ion ion-social-linkedin"
      ></a>
    </div>
  `,
})
export class FooterComponent {}
