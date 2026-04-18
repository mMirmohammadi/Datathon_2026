import { useRef, useState } from "react";

type ListingData = {
  id: string;
  title: string;
  city?: string | null;
  canton?: string | null;
  image_urls?: string[] | null;
  hero_image_url?: string | null;
  price_chf?: number | null;
  rooms?: number | null;
  features?: string[];
};

type RankedListingResult = {
  listing_id: string;
  score: number;
  reason: string;
  listing: ListingData;
};

type RankedListProps = {
  results: RankedListingResult[];
  selectedId: string | null;
  onSelect: (listingId: string) => void;
};

function formatPrice(price?: number | null): string {
  if (price == null) {
    return "Price n/a";
  }
  return new Intl.NumberFormat("de-CH", {
    style: "currency",
    currency: "CHF",
    maximumFractionDigits: 0,
  }).format(price);
}

function getImageUrls(listing: ListingData): string[] {
  const candidates = [listing.hero_image_url, ...(listing.image_urls ?? [])].filter(
    (value): value is string => Boolean(value),
  );
  return Array.from(new Set(candidates));
}

export default function RankedList({
  results,
  selectedId,
  onSelect,
}: RankedListProps) {
  const [imageIndexes, setImageIndexes] = useState<Record<string, number>>({});
  const touchStartXRef = useRef<Record<string, number>>({});

  if (!results.length) {
    return (
      <div className="empty-state">
        <p>No widget data yet.</p>
        <p className="muted">Run the `search_listings` tool to render the map and list.</p>
      </div>
    );
  }

  return (
    <div className="ranked-list">
      {results.map((result, index) => {
        const listing = result.listing;
        const features = (listing.features ?? []).slice(0, 4);
        const imageUrls = getImageUrls(listing);
        const activeImageIndex = imageIndexes[result.listing_id] ?? 0;
        const activeImageUrl =
          imageUrls[(activeImageIndex + imageUrls.length) % Math.max(imageUrls.length, 1)];

        const advanceImage = (delta: number) => {
          onSelect(result.listing_id);
          if (imageUrls.length <= 1) {
            return;
          }
          setImageIndexes((current) => {
            const currentIndex = current[result.listing_id] ?? 0;
            const nextIndex = (currentIndex + delta + imageUrls.length) % imageUrls.length;
            return { ...current, [result.listing_id]: nextIndex };
          });
        };

        return (
          <div
            key={result.listing_id}
            className={`listing-card ${selectedId === result.listing_id ? "selected" : ""}`}
            onClick={() => onSelect(result.listing_id)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelect(result.listing_id);
              }
            }}
            role="button"
            tabIndex={0}
          >
            {activeImageUrl ? (
              <div className="listing-image-wrap">
                {imageUrls.length > 1 ? (
                  <>
                    <button
                      aria-label="Show previous image"
                      className="listing-image-button listing-image-button-prev"
                      onClick={(event) => {
                        event.stopPropagation();
                        advanceImage(-1);
                      }}
                      type="button"
                    >
                      ‹
                    </button>
                    <button
                      aria-label="Show next image"
                      className="listing-image-button listing-image-button-next"
                      onClick={(event) => {
                        event.stopPropagation();
                        advanceImage(1);
                      }}
                      type="button"
                    >
                      ›
                    </button>
                    <div className="listing-image-count">
                      {activeImageIndex + 1}/{imageUrls.length}
                    </div>
                  </>
                ) : null}
                <img
                  className="listing-image"
                  src={activeImageUrl}
                  alt={listing.title}
                  loading="lazy"
                  onTouchEnd={(event) => {
                    const startX = touchStartXRef.current[result.listing_id];
                    if (startX == null) {
                      return;
                    }
                    const endX = event.changedTouches[0]?.clientX;
                    if (typeof endX !== "number") {
                      return;
                    }
                    const deltaX = endX - startX;
                    if (Math.abs(deltaX) < 36) {
                      onSelect(result.listing_id);
                      return;
                    }
                    advanceImage(deltaX < 0 ? 1 : -1);
                  }}
                  onTouchStart={(event) => {
                    const touch = event.touches[0];
                    if (touch) {
                      touchStartXRef.current[result.listing_id] = touch.clientX;
                    }
                  }}
                />
              </div>
            ) : null}
            <div className="listing-card-header">
              <span className="listing-rank">#{index + 1}</span>
              <span className="listing-score">{result.score.toFixed(2)}</span>
            </div>
            <h2>{listing.title}</h2>
            <p className="listing-meta">
              {[listing.city, listing.canton].filter(Boolean).join(", ")}
            </p>
            <p className="listing-meta">
              {formatPrice(listing.price_chf)} · {listing.rooms ?? "?"} rooms
            </p>
            <p className="listing-reason">{result.reason}</p>
            {!!features.length && (
              <div className="feature-row">
                {features.map((feature) => (
                  <span key={feature} className="feature-badge">
                    {feature.replaceAll("_", " ")}
                  </span>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
