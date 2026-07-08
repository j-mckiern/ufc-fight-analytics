default:
    @just --list

# Incrementally scrape new events, fights, and fighter profiles into data/raw
scrape:
    uv run ufc scrape

# Rebuild prepared datasets (train/test/fighter features) from data/raw
prepare:
    uv run ufc prepare

# Rebuild data and retrain the model (archives the previous bundle)
train:
    uv run ufc train

# Predict the next upcoming event's card (add --save via: just predict-card --save)
predict-card *ARGS:
    uv run ufc predict-card {{ARGS}}

# Scrape results and reconcile saved predictions
update-results *ARGS:
    uv run ufc update-results {{ARGS}}

# Show current model metadata
model-info:
    uv run ufc model-info

# Launch the Streamlit app
app:
    uv run streamlit run app/streamlit_app.py

# Run the test suite
test:
    uv run pytest

# Execute the exploration notebook in place
notebook:
    uv run jupyter nbconvert --to notebook --execute --inplace notebooks/exploration.ipynb
