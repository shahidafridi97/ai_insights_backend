import express from 'express';
import cors from 'cors';

import { getConnection, loadParquetOnce } from './db.js';

const app = express();
app.use(cors());
app.use(express.json());

let tablePromise = null;

/* ================= HELPERS ================= */

function parseMoney(value) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const cleaned = String(value ?? '').replace(/[^\d.-]/g, '');
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeRow(row) {
  return {
    ...row,
    pr: Number(row.pr),
    y: Number(row.y),
  };
}

/* ================= FULL MATCHING (UNCHANGED) ================= */

async function readRelevantRows(property) {
  const conn = await getConnection();

  const normalize = (v = '') =>
    String(v)
      .toUpperCase()
      .replace(/[^\w\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

  const cleanStreet = (v = '') =>
    String(v)
      .toUpperCase()
      .replace(/[^\w\s]/g, ' ')
      .replace(/\b\d+\w*\b/g, '')
      .replace(/\s+/g, ' ')
      .trim();

  const normalizePostcode = (pc = '') =>
    String(pc).toUpperCase().replace(/\s+/g, '').trim();

  const safe = (v = '') => v.replace(/'/g, "''");

  const inputStreet = cleanStreet(property?.display_address?.split(',')[0] || '');
  const inputArea = normalize(property?.area || '');
  const inputPostcode = normalizePostcode(property?.post_code || '');
  const inputOutward = inputPostcode.slice(0, 3);

  if (!inputStreet || !inputOutward) {
    return {
      rows: [],
      totalMatched: 0,
      counts: { exact: 0, street: 0, area: 0, total: 0 },
    };
  }

  /* ================= LOAD PARQUET ONCE ================= */

  if (!tablePromise) {
    tablePromise = new Promise(async (resolve, reject) => {
      try {
        await loadParquetOnce(conn);
        resolve(true);
      } catch (err) {
        tablePromise = null;
        reject(err);
      }
    });
  }

  await tablePromise;

  /* ================= QUERY ================= */

  const query = `
    WITH base AS (
      SELECT
        regexp_replace(upper(s), '[^A-Z0-9 ]', '', 'g') AS clean_street,
        replace(upper(pc), ' ', '') AS clean_pc,
        split_part(upper(pc), ' ', 1) AS outward,
        upper(coalesce(t, l)) AS clean_area,
        pr, y, ty, pc, s, t
      FROM properties
      WHERE pr BETWEEN 30000 AND 2000000
    )
    SELECT *
    FROM base
    WHERE
      (clean_street = '${safe(inputStreet)}' AND clean_pc = '${safe(inputPostcode)}')
      OR
      (clean_street = '${safe(inputStreet)}' AND outward = '${safe(inputOutward)}')
      OR
      (clean_area = '${safe(inputArea)}' AND outward = '${safe(inputOutward)}')
  `;

  const rawRows = await new Promise((resolve, reject) => {
    conn.all(query, (err, res) => {
      if (err) reject(err);
      else resolve(res);
    });
  });

  const rows = rawRows.map(normalizeRow);

  /* ================= CLASSIFY ================= */

  const exactMatches = [];
  const streetMatches = [];
  const areaMatches = [];

  rows.forEach((row) => {
    const cleanStreetDB = row.clean_street;
    const cleanPcDB = row.clean_pc;

    const outwardDB = row.outward;
    const microPcDB = cleanPcDB.slice(0, 5);

    const sameStreet = cleanStreetDB === inputStreet;
    const samePostcode = cleanPcDB === inputPostcode;
    const sameOutward = outwardDB === inputOutward;
    const sameArea = row.clean_area === inputArea;

    const inputMicro = inputPostcode.slice(0, 5);

    const base = {
      price: Number(row.pr),
      year: Number(row.y),
      postcode: row.pc,
      street: row.s,
      town: row.t,
      type: row.ty,
    };

    const strictPostcodeMatch =
      cleanPcDB === inputPostcode &&
      row.pc.replace(/\s+/g, '') === inputPostcode;

    if (sameStreet && strictPostcodeMatch) {
      exactMatches.push(base);
    } else if (sameStreet && sameOutward && !samePostcode) {
      streetMatches.push(base);
    } else if (sameArea && microPcDB === inputMicro && !sameStreet) {
      areaMatches.push(base);
    }
  });

  /* ================= DEDUPE ================= */

  const uniqueMap = new Map();

  [...exactMatches, ...streetMatches, ...areaMatches].forEach((r) => {
    const key = `${r.year}-${r.postcode}-${r.price}-${r.street}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, r);
  });

  const finalRows = Array.from(uniqueMap.values());

  const counts = {
    exact: exactMatches.length,
    street: streetMatches.length,
    area: areaMatches.length,
    total: finalRows.length,
  };

  console.log('📊 MATCH COUNTS:', counts);

  return {
    rows: finalRows,
    totalMatched: finalRows.length,
    allRows: finalRows,

    exactMatches,
    streetMatches,
    areaMatches,

    counts,

    matchedBy:
      exactMatches.length > 0
        ? 'Exact Match'
        : streetMatches.length > 0
        ? 'Street Match'
        : 'Area Match',

    strictMode: exactMatches.length > 0 || streetMatches.length > 0,
    strictCount: exactMatches.length + streetMatches.length,
    areaCount: areaMatches.length,
  };
}

/* ================= API ================= */

app.post('/predict', async (req, res) => {
  try {
    const property = req.body;

    const result = await readRelevantRows(property);

    return res.json({
      success: true,
      ...result,
    });

  } catch (err) {
    console.error('❌ ERROR:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ================= START ================= */

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});