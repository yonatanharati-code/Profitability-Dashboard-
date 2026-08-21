import { useEffect, useState } from "react";
import { BottomNav, type Tab } from "./components/BottomNav";
import { TodayView } from "./views/TodayView";
import { TripView } from "./views/TripView";
import { MapView } from "./views/MapView";
import { BookingsView } from "./views/BookingsView";
import { MoreView } from "./views/MoreView";
import { useTripState } from "./hooks/useTripState";
import { useNow } from "./hooks/useNow";
import { days } from "./data/days";
import { DayHeadline } from "./components/DayView";
import {
  clockInJapan,
  isoInJapan,
  minutesInJapan,
  resolveActiveDate,
} from "./utils/date";
import { load, save } from "./utils/storage";
import { ExternalLinkProvider } from "./components/ExternalLink";

export default function App() {
  const state = useTripState();
  const now = useNow();
  const today = isoInJapan(now);
  const nowMinutes = minutesInJapan(now);

  // Remember which tab you were on — reopening mid-trip should feel continuous.
  const [tab, setTab] = useState<Tab>(() => load<Tab>("tab", "today"));
  const [openDate, setOpenDate] = useState<string | null>(null);

  useEffect(() => {
    save("tab", tab);
  }, [tab]);

  // Every tab change scrolls back to the top; so does opening a day.
  useEffect(() => {
    window.scrollTo({ top: 0 });
  }, [tab, openDate]);

  const activeDay = days.find((d) => d.date === resolveActiveDate(today))!;
  const stickyDay =
    tab === "trip" && openDate
      ? days.find((d) => d.date === openDate)
      : activeDay;

  return (
    <ExternalLinkProvider>
      <div className="mx-auto flex min-h-dvh max-w-2xl flex-col bg-paper sm:border-x sm:border-sumi-100">
        {/* Sticky day header — always tells you which day you are looking at. */}
        <header className="sticky top-0 z-30 border-b border-sumi-100 bg-paper/85 backdrop-blur-xl">
          <div className="flex items-center justify-between gap-3 px-5 py-3">
            {stickyDay ? (
              <DayHeadline day={stickyDay} />
            ) : (
              <span className="eyebrow">Japan 2026</span>
            )}
            <span className="tabular shrink-0 text-[12px] font-semibold text-sumi-400">
              {clockInJapan(now).slice(0, 5)} JST
            </span>
          </div>
        </header>

        <main className="flex-1 px-5 pt-6 pb-[calc(76px+env(safe-area-inset-bottom))]">
          {tab === "today" && (
            <TodayView
              state={state}
              today={today}
              now={now}
              nowMinutes={nowMinutes}
            />
          )}
          {tab === "trip" && (
            <TripView
              state={state}
              today={today}
              nowMinutes={nowMinutes}
              openDate={openDate}
              onOpenDate={setOpenDate}
            />
          )}
          {tab === "map" && <MapView state={state} today={today} />}
          {tab === "bookings" && <BookingsView state={state} />}
          {tab === "more" && <MoreView state={state} today={today} />}
        </main>

        <BottomNav
          active={tab}
          onChange={(next) => {
            if (next === "trip" && tab === "trip") setOpenDate(null);
            setTab(next);
          }}
        />
      </div>
    </ExternalLinkProvider>
  );
}
