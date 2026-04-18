"""Multilingual prompt templates for the 7-class triage classifier.

Every label has at least 3 templates per language (DE / FR / IT / EN) so the
per-class text embedding is an average over ≥12 phrasings — cheaper, more robust
prompting than a single template (CLIP paper §3.1.4; CuPL arXiv 2209.03320).

Corpus language mix (per analysis/REPORT.md §8): DE 71% / FR 22% / IT 3.7% / EN 0.4%.
"""
from __future__ import annotations


KEPT_CLASSES: frozenset[str] = frozenset({
    "interior-room",
    "building-exterior",
    "surroundings-or-view",
    "floorplan",
})

DROPPED_CLASSES: frozenset[str] = frozenset({
    "logo-or-banner",
    "marketing-or-stock-photo",
    "other-uninformative",
})

FLOORPLAN_CLASSES: frozenset[str] = frozenset({"floorplan"})
MAIN_INDEX_CLASSES: frozenset[str] = KEPT_CLASSES - FLOORPLAN_CLASSES
ALL_CLASSES: tuple[str, ...] = tuple(sorted(KEPT_CLASSES | DROPPED_CLASSES))


# Templates per (label, language). Each template is a standalone phrase.
PROMPTS: dict[str, dict[str, list[str]]] = {
    "interior-room": {
        "de": [
            "ein Foto eines Zimmers in einem Haus",
            "Innenansicht einer Wohnung oder eines Hauses",
            "ein Bild vom Inneren eines Raumes",
            "Küche, Wohnzimmer, Schlafzimmer oder Badezimmer einer Wohnung",
        ],
        "fr": [
            "une photo d'une pièce à l'intérieur d'une maison",
            "vue intérieure d'un appartement ou d'une maison",
            "une image de l'intérieur d'une pièce",
            "cuisine, salon, chambre ou salle de bain d'un appartement",
        ],
        "it": [
            "una foto di una stanza dentro una casa",
            "vista interna di un appartamento o di una casa",
            "immagine dell'interno di una stanza",
            "cucina, soggiorno, camera da letto o bagno di un appartamento",
        ],
        "en": [
            "a photo of a room inside a house",
            "the interior of a home showing a room",
            "a picture of the inside of an apartment",
            "a kitchen, living room, bedroom, or bathroom of a home",
        ],
    },
    "building-exterior": {
        "de": [
            "ein Foto der Außenansicht eines Gebäudes",
            "die Fassade eines Wohnhauses oder einer Wohnung",
            "ein Bild eines Hauses von außen, einschließlich Dach und Balkon",
        ],
        "fr": [
            "une photo de l'extérieur d'un bâtiment",
            "la façade d'un immeuble ou d'une maison",
            "une image d'une maison vue de l'extérieur, y compris toit et balcon",
        ],
        "it": [
            "una foto dell'esterno di un edificio",
            "la facciata di un condominio o di una casa",
            "immagine di una casa vista da fuori, compreso tetto e balcone",
        ],
        "en": [
            "a photo of the exterior of a building",
            "the facade of an apartment building or house",
            "a picture of a house from the outside, including roof and balcony",
        ],
    },
    "surroundings-or-view": {
        "de": [
            "Blick aus dem Fenster auf die Umgebung",
            "Straße oder Nachbarschaft in der Nähe des Hauses",
            "Garten oder Außenanlage eines Hauses",
            "Blick auf Berge, See oder Park nahe einer Wohnung",
        ],
        "fr": [
            "vue depuis la fenêtre sur les environs",
            "rue ou quartier autour de la maison",
            "jardin ou espaces extérieurs d'une propriété",
            "vue sur les montagnes, un lac ou un parc près d'un appartement",
        ],
        "it": [
            "vista dalla finestra sui dintorni",
            "strada o quartiere intorno alla casa",
            "giardino o spazi esterni di una proprietà",
            "vista su montagne, lago o parco vicino a un appartamento",
        ],
        "en": [
            "a view from a window onto the surroundings",
            "a photo of the street or neighbourhood near the house",
            "a garden or outdoor area near a home",
            "a view of mountains, a lake, or a park near an apartment",
        ],
    },
    "floorplan": {
        "de": [
            "ein Grundriss einer Wohnung",
            "architektonischer Grundriss-Plan",
            "schematische Zeichnung der Raumaufteilung einer Wohnung",
        ],
        "fr": [
            "un plan d'étage d'un appartement",
            "plan architectural d'un logement",
            "dessin schématique de la disposition des pièces",
        ],
        "it": [
            "una planimetria di un appartamento",
            "piano architettonico di un alloggio",
            "disegno schematico della disposizione delle stanze",
        ],
        "en": [
            "an architectural floorplan diagram of an apartment",
            "a schematic drawing of the room layout of a home",
            "a 2D floor plan showing rooms and walls from above",
        ],
    },
    "logo-or-banner": {
        "de": [
            "ein Firmenlogo oder Markenlogo",
            "ein Text-Banner oder reines Textbild",
            "ein Bild, das hauptsächlich aus Text oder einer Marke besteht",
        ],
        "fr": [
            "un logo d'entreprise ou une marque",
            "une bannière de texte ou image purement textuelle",
            "une image composée principalement de texte ou d'un logo",
        ],
        "it": [
            "un logo aziendale o marchio",
            "un banner testuale o immagine puramente testuale",
            "un'immagine composta principalmente da testo o logo",
        ],
        "en": [
            "a company logo or brand mark",
            "a text-only banner or purely textual image",
            "an image consisting mainly of text or a brand logo",
        ],
    },
    "marketing-or-stock-photo": {
        "de": [
            "ein Werbefoto oder Stockbild mit überlagertem Text",
            "ein Marketingbild einer Immobilienagentur, nicht der Wohnung",
            "ein Stockfoto mit Bewertungs-Sternen oder Agentur-Branding",
        ],
        "fr": [
            "une photo publicitaire ou image stock avec du texte superposé",
            "une photo marketing d'une agence immobilière, pas du logement",
            "une photo stock avec des étoiles de notation ou le branding d'une agence",
        ],
        "it": [
            "una foto pubblicitaria o immagine stock con testo sovrapposto",
            "una foto marketing di un'agenzia immobiliare, non dell'appartamento",
            "una foto stock con stelle di valutazione o branding di agenzia",
        ],
        "en": [
            "a marketing or stock photo with overlaid text",
            "a promotional image of a real-estate agency, not of the apartment",
            "a stock photo with rating stars or agency branding overlaid",
        ],
    },
    "other-uninformative": {
        "de": [
            "ein irrelevantes oder nichtssagendes Bild",
            "ein Bild, das keine Information über ein Haus liefert",
            "dekoratives oder fehlerhaftes Bild",
        ],
        "fr": [
            "une image non pertinente ou sans information",
            "une image qui ne donne aucune information sur une maison",
            "image décorative ou défectueuse",
        ],
        "it": [
            "un'immagine irrilevante o non informativa",
            "un'immagine che non fornisce informazioni su una casa",
            "immagine decorativa o difettosa",
        ],
        "en": [
            "an unrelated or uninformative image",
            "a picture that gives no information about a house",
            "a decorative or corrupt image",
        ],
    },
}


def flatten(label: str) -> list[str]:
    """Return every template for a label, across all languages, flat."""
    out: list[str] = []
    for lang in ("de", "fr", "it", "en"):
        out.extend(PROMPTS[label][lang])
    return out
