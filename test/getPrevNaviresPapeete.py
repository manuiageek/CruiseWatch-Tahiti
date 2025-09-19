"""Scraper Playwright pour les prÃ©visions de navires Ã  Papeete.

Ce script ouvre la page publique et extrait le tableau principal des prÃ©visions
de navires (y compris si le tableau se trouve dans un iframe). Les donnÃ©es
peuvent Ãªtre affichÃ©es en JSON sur stdout, ou exportÃ©es en CSV/JSON via options.

Exemples d'exÃ©cution:
  - Afficher en JSON:  python getPrevNaviresPapeete.py
  - Exporter fichiers: python getPrevNaviresPapeete.py --json out.json --csv out.csv
  - Debug dÃ©taillÃ©:    python getPrevNaviresPapeete.py --log-level DEBUG --headful

Notes:
  - Le script choisit automatiquement le Â« meilleur Â» tableau selon un score
    simple (nb de lignes/colonnes + prÃ©sence d'entÃªtes).
  - Les logs sont configurables via --log-level (DEBUG Ã  CRITICAL).
"""

import argparse
import csv
import json
import logging
import sys
import time
from typing import Any, Dict, List, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


logger = logging.getLogger(__name__)

# Liste des en-têtes à ignorer dans la sortie
IGNORED_HEADERS = {
    "N° Escale",
    "N° Voyage",
    "agent",
    "acconier",
}

# Normalise un en-tête: minuscule, espaces uniques, retire les caractères °/º
def _normalize_header(name: str) -> str:
    base = " ".join((name or "").lower().split())
    return base.replace("\u00b0", "").replace("\u00ba", "").strip()

# Indique si un en-tête doit être ignoré
def _should_ignore_header(name: str) -> bool:
    norm = _normalize_header(name)
    for h in IGNORED_HEADERS:
        if _normalize_header(h) == norm:
            return True
    return False




# Recupere les tableaux detectes dans un frame Playwright.
def _collect_tables_from_frame(frame) -> List[Dict[str, Any]]:
    """Collecte tous les tableaux HTML prÃ©sents dans un frame.

    Retourne une liste de dictionnaires contenant:
      - headers: en-tÃªtes dÃ©tectÃ©s (si disponibles)
      - rows: lignes du tableau (listes de cellules en texte)
      - rowCount/colCount: dimensions estimÃ©es
      - score: mÃ©trique simple pour classer les tableaux
    """
    try:
        tables = frame.evaluate(
            """
() => {
  // Petite helper pour normaliser le texte d'une cellule
  function getText(cell) {
    return cell.innerText.trim().replace(/\s+/g, ' ');
  }

  const results = [];
  // RÃ©cupÃ¨re tous les <table> du document
  const nodeTables = Array.from(document.querySelectorAll('table'));
  for (const tbl of nodeTables) {
    const caption = tbl.caption ? tbl.caption.innerText.trim() : null;
    const id = tbl.id || null;
    const classes = tbl.className || null;

    const headerRows = Array.from(tbl.querySelectorAll('thead tr'));
    let headers = [];
    if (headerRows.length) {
      // Utilise la derniÃ¨re ligne d'entÃªte (souvent la plus dÃ©taillÃ©e)
      headers = Array.from(headerRows[headerRows.length - 1].cells).map(getText);
    } else {
      const thRow = tbl.querySelector('tr th') ? tbl.querySelector('tr th').parentElement : null;
      if (thRow) {
        headers = Array.from(thRow.cells).map(getText);
      }
    }

    let bodyTrs = Array.from(tbl.querySelectorAll('tbody tr'));
    if (bodyTrs.length === 0) {
      bodyTrs = Array.from(tbl.querySelectorAll('tr'));
      if (headers.length) {
        // Retire la premiÃ¨re ligne d'entÃªte si prÃ©sente
        const headerIndex = bodyTrs.findIndex(tr => tr.querySelector('th'));
        if (headerIndex !== -1) bodyTrs.splice(headerIndex, 1);
      }
    }

    const rows = bodyTrs
      .map(tr => Array.from(tr.cells).map(getText))
      .filter(r => r.length);

    const colCount = rows[0] ? rows[0].length : (headers.length || 0);
    const rowCount = rows.length;
    // Score simple: favorise les grands tableaux et la prÃ©sence d'en-tÃªte
    const score = rowCount * (colCount || 1) + (headers.length ? 5 : 0);
    results.push({ caption, id, classes, headers, rows, rowCount, colCount, score });
  }
  return results;
}
"""
        )
        return tables or []
    except Exception:
        logger.debug("Aucune table ou erreur lors de l'Ã©valuation dans le frame: %s", frame.url)
        return []


# Selectionne le meilleur tableau a partir des candidats.
def _find_best_table(page) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Parcourt la page et tous ses frames pour trouver le meilleur tableau.

    Renvoie un tuple (tableau_sÃ©lectionnÃ©, tous_les_candidats). Si aucun tableau
    plausible n'est trouvÃ©, renvoie (None, []).
    """
    candidates: List[Dict[str, Any]] = []
    for frame in page.frames:
        tables = _collect_tables_from_frame(frame)
        for t in tables:
            t["frame_url"] = frame.url
        logger.debug("%d table(s) dÃ©tectÃ©e(s) dans le frame: %s", len(tables), frame.url)
        candidates.extend(tables)

    plausible = [
        t
        for t in candidates
        if t.get("rowCount", 0) >= 1
        and (t.get("colCount", 0) >= 2 or (t.get("headers") and len(t.get("headers")) >= 2))
    ]
    # Si aucun tableau Â« plausible Â», on retombe sur tous les candidats
    if not plausible and candidates:
        plausible = candidates

    if not plausible:
        return None, []

    # Classement dÃ©croissant par score, puis par dimensions
    plausible.sort(key=lambda t: (t.get("score", 0), t.get("rowCount", 0), t.get("colCount", 0)), reverse=True)
    return plausible[0], plausible


# Convertit le tableau retenu en dictionnaires lignes/colonnes.
def _to_records(table: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, str]]]:
    """Convertit un tableau brut en enregistrements structurÃ©s.

    - Si aucun en-tÃªte n'est dÃ©tectÃ©, gÃ©nÃ¨re des noms Â« col_1 Â», Â« col_2 Â», ...
    - Tronque/concatÃ¨ne les cellules excÃ©dentaires pour s'aligner sur les entÃªtes
    """
    headers = table.get("headers") or []
    rows: List[List[str]] = table.get("rows", [])
    if not headers:
        maxcols = max((len(r) for r in rows), default=0)
        headers = [f"col_{i+1}" for i in range(maxcols)]

    # Indices des colonnes à conserver après filtrage
    # Indices des colonnes à conserver après filtrage
    orig_len = len(headers)
    keep_indices = [i for i, h in enumerate(headers) if not _should_ignore_header(h)]
    headers = [headers[i] for i in keep_indices]
    records: List[Dict[str, str]] = []
    for r in rows:
        if len(r) < orig_len:
            r = r + [""] * (orig_len - len(r))
        elif len(r) > orig_len:
            r = r[: orig_len - 1] + [" | ".join(r[orig_len - 1 :])]
        r = [r[i] for i in keep_indices]
        records.append(dict(zip(headers, r)))
    return headers, records


# Point dentree CLI qui orchestre le scraping et les sorties.
def main() -> None:
    """Point d'entrÃ©e CLI: parse les arguments, lance le navigateur, extrait et exporte."""
    parser = argparse.ArgumentParser(
        description="Scraper Playwright pour la page 'PrÃ©visions navires' (Port de Papeete)."
    )
    parser.add_argument(
        "--url",
        default="https://www.portdepapeete.pf/fr/previsions-navires",
        help="URL Ã  parser",
    )
    parser.add_argument("--timeout", type=int, default=45000, help="Timeout navigation en ms")
    parser.add_argument("--headful", action="store_true", help="Lancer le navigateur en mode visible")
    parser.add_argument("--csv", help="Chemin de sortie CSV")
    parser.add_argument("--json", help="Chemin de sortie JSON")
    parser.add_argument(
        "--print", action="store_true", help="Afficher les enregistrements dans la console"
    )
    # Filtrage par type (par dÃ©faut: PAQUEBOT). Utiliser --no-type-filter pour tout garder
    parser.add_argument(
        "--type-only",
        default="PAQUEBOT",
        help="Filtrer sur un type (dÃ©faut: PAQUEBOT)",
    )
    parser.add_argument(
        "--no-type-filter",
        action="store_true",
        help="DÃ©sactive le filtrage par type",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Niveau de logs (dÃ©faut: INFO)",
    )

    args = parser.parse_args()

    # Configure logging
    # Configuration simple du logging (niveau et format)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(message)s",
    )
    logger.debug("Arguments: %s", vars(args))

    with sync_playwright() as p:
        # Lancement de Chromium (headless par dÃ©faut) et crÃ©ation d'un contexte/page
        logger.info("DÃ©marrage de Chromium (headless=%s)", str(not args.headful).lower())
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()
        try:
            logger.info("Navigation vers l'URL: %s", args.url)
            # On attend le state Â« networkidle Â» pour maximiser les chances que
            # le tableau soit peuplÃ©. Certains contenus peuvent encore arriver ensuite.
            page.goto(args.url, wait_until="networkidle", timeout=args.timeout)
            logger.debug("Navigation terminÃ©e (networkidle)")
        except PlaywrightTimeoutError:
            logger.warning(
                "Timeout atteint lors du chargement de la page; tentative de rÃ©cupÃ©ration des donnÃ©es malgrÃ© tout."
            )

        best = None
        for _ in range(4):
            logger.debug("Recherche de tableaux dans la page et les iframes...")
            best, _all = _find_best_table(page)
            if best:
                logger.info(
                    "Tableau sÃ©lectionnÃ©: id=%s, classes=%s, lignes=%s, colonnes=%s",
                    best.get("id"),
                    best.get("classes"),
                    best.get("rowCount"),
                    best.get("colCount"),
                )
                break
            time.sleep(1.0)

        if not best:
            logger.error("Aucun tableau dÃ©tectÃ© sur la page (ou dans ses iframes).")
            context.close()
            browser.close()
            sys.exit(2)

        # Mise en forme des donnÃ©es (entÃªtes + enregistrements)
        headers, records = _to_records(best)
        logger.info("Extraction terminÃ©e: %d enregistrements", len(records))
        # DÃ©termine la colonne Â« type Â» et applique le filtrage si demandÃ©
        # Repere la colonne portante pour le type de navire.
        def _guess_type_field(hdrs: List[str]):
            for h in hdrs:
                if "type" in h.lower():
                    return h
            return None
        type_field = _guess_type_field(headers)
        if type_field:
            logger.debug("Colonne de type dÃ©tectÃ©e: %s", type_field)
        else:
            logger.warning("Aucune colonne contenant 'type' dÃ©tectÃ©e: filtrage ignorÃ©")
        if not args.no_type_filter and type_field:
            before = len(records)
            wanted = (args.type_only or "").strip().upper()
            if wanted:
                records = [r for r in records if r.get(type_field, "").strip().upper() == wanted]
                logger.info("Filtrage par type='%s': %d -> %d enregistrements", wanted, before, len(records))
            else:
                logger.debug("Type cible vide: aucun filtrage appliquÃ©")
        meta = {
            "source_url": args.url,
            "frame_url": best.get("frame_url"),
            "headers": headers,
            "row_count": len(records),
            "table_id": best.get("id"),
            "table_classes": best.get("classes"),
            "table_caption": best.get("caption"),
        }

        if args.json:
            logger.info("Ã‰criture JSON: %s", args.json)
            with open(args.json, "w", encoding="utf-8") as f:
                json.dump({"meta": meta, "records": records}, f, ensure_ascii=False, indent=2)

        if args.csv:
            logger.info("Ã‰criture CSV: %s", args.csv)
            with open(args.csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for rec in records:
                    writer.writerow(rec)

        if args.print or (not args.json and not args.csv):
            logger.debug("Affichage JSON sur stdout")
            print(json.dumps({"meta": meta, "records": records}, ensure_ascii=False, indent=2))

        logger.debug("Fermeture du contexte et du navigateur")
        context.close()
        browser.close()


if __name__ == "__main__":
    main()
