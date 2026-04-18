import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

type ListingData = {
  id: string;
  title: string;
  city?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  price_chf?: number | null;
};

type RankedListingResult = {
  listing_id: string;
  score: number;
  listing: ListingData;
};

type ListingsMapProps = {
  results: RankedListingResult[];
  selectedId: string | null;
  selectedListing: RankedListingResult | null;
  onSelect: (listingId: string) => void;
};

const MAP_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    "carto-positron": {
      type: "raster",
      tiles: [
        "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
        "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
        "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
        "https://d.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
      ],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; CARTO',
    },
  },
  layers: [
    {
      id: "carto-positron-layer",
      type: "raster",
      source: "carto-positron",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

export default function ListingsMap({
  results,
  selectedId,
  selectedListing,
  onSelect,
}: ListingsMapProps) {
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const markersRef = useRef<maplibregl.Marker[]>([]);

  const coordinateResults = results.filter(
    (result) =>
      typeof result.listing.latitude === "number" &&
      typeof result.listing.longitude === "number",
  );

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    mapRef.current = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      center: [8.54, 47.37],
      zoom: 7,
      attributionControl: false,
    });

    mapRef.current.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    return () => {
      markersRef.current.forEach((marker) => marker.remove());
      markersRef.current = [];
      mapRef.current?.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    markersRef.current.forEach((marker) => marker.remove());
    markersRef.current = [];

    const map = mapRef.current;
    if (!map) {
      return;
    }

    coordinateResults.forEach((result, index) => {
      const el = document.createElement("button");
      el.type = "button";
      el.className = `map-pin ${selectedId === result.listing_id ? "selected" : ""}`;
      el.textContent = String(index + 1);
      el.onclick = () => onSelect(result.listing_id);

      const marker = new maplibregl.Marker({ element: el })
        .setLngLat([result.listing.longitude!, result.listing.latitude!])
        .setPopup(
          new maplibregl.Popup({ offset: 12 }).setHTML(
            `<strong>${result.listing.title}</strong><br/>${result.listing.city ?? ""}`,
          ),
        )
        .addTo(map);

      markersRef.current.push(marker);
    });

    if (coordinateResults.length) {
      const bounds = new maplibregl.LngLatBounds();
      coordinateResults.forEach((result) => {
        bounds.extend([result.listing.longitude!, result.listing.latitude!]);
      });
      map.fitBounds(bounds, { padding: 60, maxZoom: 13, duration: 0 });
    }
  }, [coordinateResults, onSelect, selectedId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !selectedListing) {
      return;
    }
    if (
      typeof selectedListing.listing.latitude === "number" &&
      typeof selectedListing.listing.longitude === "number"
    ) {
      map.easeTo({
        center: [selectedListing.listing.longitude, selectedListing.listing.latitude],
        zoom: Math.max(map.getZoom(), 12),
        duration: 500,
      });
    }
  }, [selectedListing]);

  if (!coordinateResults.length) {
    return (
      <div className="map-empty-state">
        <p>No coordinates available for the current result set.</p>
      </div>
    );
  }

  return <div ref={mapContainerRef} className="map-container" />;
}
