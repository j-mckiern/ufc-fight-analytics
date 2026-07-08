"""Command-line entry points: ufc <command>."""

import argparse
import json

from ufc import config


def cmd_scrape(args) -> None:
    from ufc.scraping.pipeline import scrape_new

    scrape_new()


def cmd_prepare(args) -> None:
    from ufc.features.build import build_training_data, save_prepared

    data = build_training_data()
    out = save_prepared(data)
    print(f"Prepared data written to {out}")
    print(f"  train: {len(data.train)} rows (before {data.params.cutoff})")
    print(f"  test:  {len(data.test)} rows")
    print(f"  fighters: {len(data.fighter_features)}")
    print(f"  features: {len(data.params.feature_cols)}")


def cmd_train(args) -> None:
    from ufc.features.build import build_training_data, save_prepared
    from ufc.modeling.train import train_and_save

    data = build_training_data()
    save_prepared(data)
    metadata = train_and_save(data)
    print(f"Trained {len(metadata['metrics'])} candidates "
          f"(train {metadata['train_rows']} / test {metadata['test_rows']} rows, "
          f"cutoff {metadata['cutoff']}):")
    for name, m in metadata["metrics"].items():
        marker = " <- saved" if name == metadata["model_name"] else ""
        print(f"  {name:22s} acc {m['accuracy']:.3f}  auc {m['auc']:.3f}{marker}")
    print(f"Bundle saved to {config.CURRENT_MODEL_DIR} "
          f"(version {metadata['trained_at']})")


def _print_card_results(results: list[dict]) -> None:
    for r in results:
        if r["status"] == "ok":
            a, b = r["fighter_a_name"], r["fighter_b_name"]
            mark_a = "*" if r["prob_a"] > 0.5 else " "
            mark_b = "*" if r["prob_a"] <= 0.5 else " "
            print(f"  {mark_a}{a:28s} {r['prob_a']:6.1%}  vs  "
                  f"{1 - r['prob_a']:6.1%} {b}{mark_b} ({r['weight_class']})")
        else:
            print(f"   {r['fighter_a_name']} vs {r['fighter_b_name']}: "
                  f"no prediction — {r['reason']}")


def cmd_predict_card(args) -> None:
    from ufc.features.build import load_fighter_features
    from ufc.modeling.predict import predict_card
    from ufc.modeling.registry import load_model_bundle
    from ufc.scraping.http import make_session
    from ufc.scraping.upcoming import scrape_upcoming_card, scrape_upcoming_events
    from ufc.tracking.store import append_predictions

    session = make_session()
    events = scrape_upcoming_events(session)
    if not events:
        print("No upcoming events found.")
        return
    if args.event_id:
        matches = [e for e in events if e["event_id"] == args.event_id]
        if not matches:
            print(f"Event {args.event_id} not found. Upcoming events:")
            for e in events:
                print(f"  {e['event_id']}  {e['date']}  {e['name']}")
            return
        event = matches[0]
    else:
        event = events[0]

    print(f"{event['name']} — {event['date']}")
    bouts = scrape_upcoming_card(event["url"], session)
    if not bouts:
        print("No bouts listed yet.")
        return

    bundle = load_model_bundle()
    fighter_features = load_fighter_features()
    results = predict_card(bundle, fighter_features, bouts, event_date=event["date"])
    _print_card_results(results)

    if args.save:
        added = append_predictions(results, event)
        print(f"\nSaved {added} predictions to {config.PREDICTIONS_CSV}")
    else:
        print("\n(run with --save to record these predictions)")


def cmd_update_results(args) -> None:
    from ufc.scraping.pipeline import scrape_new
    from ufc.tracking.reconcile import accuracy_summary, reconcile
    from ufc.tracking.store import load_predictions, save_predictions

    if not args.no_scrape:
        scrape_new()

    predictions = load_predictions()
    if len(predictions) == 0:
        print("No saved predictions yet. Run `ufc predict-card --save` first.")
        return

    predictions, matched = reconcile(predictions)
    save_predictions(predictions)
    print(f"\nMatched {matched} pending predictions to results.")

    recent = predictions[predictions["reconciled_at"].notna()].sort_values(
        "event_date", ascending=False
    )
    for _, r in recent.head(20).iterrows():
        if r["status"] == "reconciled":
            outcome = "HIT " if r["correct"] else "MISS"
            winner = (
                r["fighter_a_name"]
                if str(r["actual_winner_id"]) == str(r["fighter_a_id"])
                else r["fighter_b_name"]
            )
            print(f"  {outcome} {r['event_date']}  {r['fighter_a_name']} vs "
                  f"{r['fighter_b_name']} -> {winner} ({r['actual_method']})")
        elif r["status"] == "no_result":
            print(f"  ---- {r['event_date']}  {r['fighter_a_name']} vs "
                  f"{r['fighter_b_name']} -> {r['actual_method']} (excluded)")

    stats = accuracy_summary(predictions)
    if stats["n"]:
        print(f"\nLifetime: {stats['accuracy']:.1%} accuracy, "
              f"Brier {stats['brier']:.3f} over {stats['n']} predictions")
    pending = int((predictions["status"] == "pending").sum())
    if pending:
        print(f"{pending} predictions still pending (events not yet fought/scraped).")


def cmd_model_info(args) -> None:
    from ufc.modeling.registry import load_model_bundle

    bundle = load_model_bundle()
    meta = {k: v for k, v in bundle.metadata.items() if k != "prep_params"}
    print(json.dumps(meta, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="ufc", description="UFC fight analytics")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "scrape", help="Incrementally scrape new events into data/raw"
    ).set_defaults(func=cmd_scrape)
    sub.add_parser("prepare", help="Build prepared datasets from data/raw").set_defaults(
        func=cmd_prepare
    )
    sub.add_parser("train", help="Rebuild data and train the model").set_defaults(
        func=cmd_train
    )
    predict_card = sub.add_parser(
        "predict-card", help="Predict an upcoming event's card"
    )
    predict_card.add_argument("--event-id", help="Upcoming event id (default: next)")
    predict_card.add_argument(
        "--save", action="store_true", help="Record predictions for later review"
    )
    predict_card.set_defaults(func=cmd_predict_card)

    update_results = sub.add_parser(
        "update-results", help="Scrape results and reconcile saved predictions"
    )
    update_results.add_argument(
        "--no-scrape", action="store_true", help="Reconcile against existing data only"
    )
    update_results.set_defaults(func=cmd_update_results)

    sub.add_parser("model-info", help="Show current model metadata").set_defaults(
        func=cmd_model_info
    )

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
