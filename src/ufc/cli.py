"""Command-line entry points: ufc <command>."""

import argparse
import json

from ufc import config


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


def cmd_model_info(args) -> None:
    from ufc.modeling.registry import load_model_bundle

    bundle = load_model_bundle()
    meta = {k: v for k, v in bundle.metadata.items() if k != "prep_params"}
    print(json.dumps(meta, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="ufc", description="UFC fight analytics")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("prepare", help="Build prepared datasets from data/raw").set_defaults(
        func=cmd_prepare
    )
    sub.add_parser("train", help="Rebuild data and train the model").set_defaults(
        func=cmd_train
    )
    sub.add_parser("model-info", help="Show current model metadata").set_defaults(
        func=cmd_model_info
    )

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
