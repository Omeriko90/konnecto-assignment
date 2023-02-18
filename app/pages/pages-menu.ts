import { NbMenuItem } from "@nebular/theme";

export const MENU_ITEMS: NbMenuItem[] = [
  {
    title: "Segments",
    icon: "layout-outline",
    children: [
      {
        title: "List",
        link: "/pages/segments/list",
      },
    ],
  },
  {
    title: "Empty Section",
    icon: "layout-outline",
    children: [
      {
        title: "Empty",
        link: "/pages/empty",
      },
    ],
  },
];
