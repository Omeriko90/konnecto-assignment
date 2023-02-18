import { Component, OnDestroy, OnInit } from "@angular/core";
import {
  NbMediaBreakpointsService,
  NbMenuService,
  NbSidebarService,
  NbThemeService,
} from "@nebular/theme";
import { map, takeUntil } from "rxjs/operators";
import { Subject, Observable } from "rxjs";

import { UserData } from "../../../@core/data/users";
import { LayoutService } from "../../../@core/utils";
import { RippleService } from "../../../@core/utils/ripple.service";
import { Router } from "@angular/router";

@Component({
  selector: "ngx-header",
  styleUrls: ["./header.component.scss"],
  templateUrl: "./header.component.html",
})
export class HeaderComponent implements OnInit, OnDestroy {
  private destroy$: Subject<void> = new Subject<void>();
  public readonly materialTheme$: Observable<boolean>;
  userPictureOnly = false;
  user: any;
  themes = [
    {
      value: "default",
      name: "Light",
    },
    {
      value: "dark",
      name: "Dark",
    },
    {
      value: "cosmic",
      name: "Cosmic",
    },
    {
      value: "corporate",
      name: "Corporate",
    },
    {
      value: "material-light",
      name: "Material Light",
    },
    {
      value: "material-dark",
      name: "Material Dark",
    },
  ];
  currentTheme = "default";
  userMenu = [{ title: "Profile" }, { title: "Log out", data: "logout" }];

  public constructor(
    private sidebarService: NbSidebarService,
    private menuService: NbMenuService,
    private themeService: NbThemeService,
    private userService: UserData,
    private layoutService: LayoutService,
    private breakpointService: NbMediaBreakpointsService,
    private rippleService: RippleService,
    private router: Router
  ) {
    this.materialTheme$ = this.themeService.onThemeChange().pipe(
      map((theme) => {
        const themeName: string = theme?.name || "";
        return themeName.startsWith("material");
      })
    );
  }

  ngOnInit(): void {
    this.currentTheme = this.themeService.currentTheme;

    const { xl } = this.breakpointService.getBreakpointsMap();
    this.themeService
      .onMediaQueryChange()
      .pipe(
        map(([, currentBreakpoint]) => currentBreakpoint.width < xl),
        takeUntil(this.destroy$)
      )
      .subscribe(
        (isLessThanXl: boolean) => (this.userPictureOnly = isLessThanXl)
      );

    this.themeService
      .onThemeChange()
      .pipe(
        map(({ name }) => name),
        takeUntil(this.destroy$)
      )
      .subscribe((themeName) => {
        this.currentTheme = themeName;
        this.rippleService.toggle(themeName?.startsWith("material"));
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  changeTheme(themeName: string): void {
    this.themeService.changeTheme(themeName);
  }

  toggleSidebar(): boolean {
    this.sidebarService.toggle(true, "menu-sidebar");
    this.layoutService.changeLayoutSize();

    return false;
  }

  navigateHome(): boolean {
    this.menuService.navigateHome();
    return false;
  }
}
