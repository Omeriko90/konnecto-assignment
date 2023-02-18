import { ComponentFixture, TestBed } from "@angular/core/testing";

import { SegmentDataComponent } from "./segment-data.component";

describe("SegmentDataComponent", () => {
  let component: SegmentDataComponent;
  let fixture: ComponentFixture<SegmentDataComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SegmentDataComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SegmentDataComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
