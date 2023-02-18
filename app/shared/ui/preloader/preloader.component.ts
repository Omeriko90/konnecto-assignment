import { Component, Input, OnInit } from "@angular/core";

@Component({
  selector: "app-ui-preloader",
  templateUrl: "./preloader.component.html",
  styleUrls: ["./preloader.component.scss"],
})
export class PreloaderComponent implements OnInit {
  @Input() display = false;

  constructor() {}

  ngOnInit(): void {}

  /**
   * Shows the loader
   */
  show(): void {
    this.display = true;
  }

  /**
   * Hides the loader
   */
  hide(): void {
    this.display = false;
  }
}
