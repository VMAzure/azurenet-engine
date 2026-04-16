# azurenet-engine — Dockerfile esplicito per Railway
#
# Sostituisce nixpacks.toml perché Nixpacks (sia con aptPkgs che con nixPkgs)
# non stava installando ffmpeg nel runtime container: i podcast worker cadevano
# nel fallback WAV, che Supabase Storage rigetta con 400.
#
# Con Dockerfile esplicito ffmpeg viene installato via apt in un singolo stage
# e resta nel PATH del CMD finale — comportamento deterministico.

FROM python:3.13-slim

# ffmpeg per vehicle_podcast_worker e dealer_podcast_worker (PCM → MP3 128k)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
