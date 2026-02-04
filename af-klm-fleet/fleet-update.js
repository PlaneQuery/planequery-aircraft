#!/usr/bin/env node

/**
 * Air France / KLM Fleet Catalog Updater
 * 
 * Standalone script to update AF.json or KL.json without a database.
 * Fetches flights from the Air France/KLM API and updates the catalog.
 * 
 * Usage:
 *   node fleet-update.js --airline AF              # Update Air France
 *   node fleet-update.js --airline KL              # Update KLM
 *   node fleet-update.js --airline KL --bootstrap  # Build from scratch (7 days)
 *   node fleet-update.js --airline KL --dry-run    # Preview changes
 * 
 * Environment:
 *   AFKLM_API_KEY  - Single API key for Air France/KLM API
 *   AFKLM_API_KEYS - Comma-separated API keys (for rotation)
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Airline metadata
const AIRLINES = {
  AF: {
    code: 'AF',
    name: 'Air France',
    country: 'France',
    registrationPrefix: 'F-',
  },
  KL: {
    code: 'KL',
    name: 'KLM Royal Dutch Airlines',
    country: 'Netherlands',
    registrationPrefix: 'PH-',
  },
};

// Configuration (loaded dynamically)
let CONFIG = {
  apiKeys: [],
  baseUrl: 'https://api.airfranceklm.com/opendata',
  pageSize: 100,
  requestDelay: 5000,
  catalogPath: null,
  airlineCode: null,
};

// Track API usage
let currentKeyIndex = 0;
let lastRequestTime = 0;
let totalRequests = 0;

// ============================================================================
// API Functions
// ============================================================================

function getApiKey() {
  return CONFIG.apiKeys[currentKeyIndex];
}

function rotateKey() {
  currentKeyIndex = (currentKeyIndex + 1) % CONFIG.apiKeys.length;
  return getApiKey();
}

async function throttle() {
  const now = Date.now();
  const timeSince = now - lastRequestTime;
  if (timeSince < CONFIG.requestDelay) {
    await new Promise(r => setTimeout(r, CONFIG.requestDelay - timeSince));
  }
  lastRequestTime = Date.now();
}

async function apiRequest(endpoint, params = {}, retryCount = 0) {
  await throttle();
  totalRequests++;

  const url = new URL(`${CONFIG.baseUrl}${endpoint}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.append(key, value);
    }
  });

  // Rotate key before each request
  if (CONFIG.apiKeys.length > 1 && retryCount === 0) {
    rotateKey();
  }

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'API-Key': getApiKey(),
      'Accept': 'application/hal+json',
      'Accept-Language': 'en-GB',
    },
  });

  if (!response.ok) {
    // Retry on rate limit (silently rotate key)
    if ((response.status === 429 || response.status === 403) && retryCount < CONFIG.apiKeys.length - 1) {
      rotateKey();
      await new Promise(r => setTimeout(r, 1000));
      return apiRequest(endpoint, params, retryCount + 1);
    }
    throw new Error(`API Error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

// ============================================================================
// Data Extraction
// ============================================================================

function extractAircraftFromFlight(flight, airlineCode) {
  const leg = flight.flightLegs?.[0];
  if (!leg?.aircraft?.registration) return null;

  const aircraft = leg.aircraft;
  
  // Filter by owner airline
  if (aircraft.ownerAirlineCode !== airlineCode) return null;
  
  return {
    registration: aircraft.registration,
    typeCode: aircraft.typeCode || null,
    typeName: aircraft.typeName || null,
    subFleetCode: aircraft.subFleetCodeId || null,
    ownerAirlineCode: aircraft.ownerAirlineCode || null,
    ownerAirlineName: aircraft.ownerAirlineName || null,
    cabinCrewEmployer: aircraft.cabinCrewEmployer || null,
    cockpitCrewEmployer: aircraft.cockpitCrewEmployer || null,
    wifiEnabled: aircraft.wifiEnabled || null,
    highSpeedWifi: aircraft.highSpeedWifi || null,
    satelliteConnectivity: aircraft.satelliteConnectivityOnBoard || null,
    physicalPaxConfiguration: aircraft.physicalPaxConfiguration || null,
  };
}

function parseCabinConfig(config) {
  if (!config) return { first: 0, business: 0, premium_economy: 0, economy: 0 };
  
  // P/F = First, J/C = Business, W/S = Premium Economy, Y/M = Economy
  const mapping = { 
    P: 'first', F: 'first', 
    J: 'business', C: 'business', 
    W: 'premium_economy', S: 'premium_economy',
    Y: 'economy', M: 'economy'
  };
  const classes = { first: 0, business: 0, premium_economy: 0, economy: 0 };
  
  const regex = /([PFJCWSYM])(\d{2,3})/g;
  let match;
  while ((match = regex.exec(config)) !== null) {
    const classKey = mapping[match[1]];
    if (classKey) classes[classKey] += parseInt(match[2], 10);
  }
  
  return classes;
}

function convertWifi(wifiEnabled, highSpeedWifi) {
  if (wifiEnabled !== 'Y') return 'none';
  if (highSpeedWifi === 'Y') return 'high-speed';
  return 'low-speed';
}

function transformToSchema(raw, firstSeenDate) {
  const cabinClasses = parseCabinConfig(raw.physicalPaxConfiguration);
  
  return {
    registration: raw.registration,
    icao24: null,
    
    aircraft_type: {
      iata_code: raw.typeCode,
      icao_code: null,
      manufacturer: guessManufacturer(raw.typeName),
      model: guessModel(raw.typeName),
      variant: guessVariant(raw.typeName),
      full_name: raw.typeName,
    },
    
    operator: {
      sub_fleet_code: raw.subFleetCode,
      cabin_crew_employer: raw.cabinCrewEmployer,
      cockpit_crew_employer: raw.cockpitCrewEmployer,
    },
    
    cabin: {
      physical_configuration: raw.physicalPaxConfiguration,
      saleable_configuration: null,
      total_seats: Object.values(cabinClasses).reduce((a, b) => a + b, 0) || null,
      classes: cabinClasses,
      freight_configuration: null,
    },
    
    connectivity: {
      wifi: convertWifi(raw.wifiEnabled, raw.highSpeedWifi),
      wifi_provider: raw.highSpeedWifi === 'Y' ? 'Starlink' : null,
      satellite: raw.satelliteConnectivity === 'Y',
    },
    
    status: 'active',
    
    tracking: {
      first_seen: firstSeenDate,
      last_seen: firstSeenDate,
      total_flights: 1,
    },
    
    metadata: {
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    
    history: [],
  };
}

function guessManufacturer(typeName) {
  if (!typeName) return null;
  if (typeName.toUpperCase().includes('AIRBUS')) return 'Airbus';
  if (typeName.toUpperCase().includes('BOEING')) return 'Boeing';
  if (typeName.toUpperCase().includes('EMBRAER')) return 'Embraer';
  return null;
}

function guessModel(typeName) {
  if (!typeName) return null;
  const match = typeName.match(/A(\d{3})|(\d{3})/);
  if (match) return match[1] ? `A${match[1]}` : match[2];
  return null;
}

function guessVariant(typeName) {
  if (!typeName) return null;
  const match = typeName.match(/-(\d+)/);
  return match ? match[1] : null;
}

function formatDate(date) {
  return date.toISOString().split('T')[0];
}

// ============================================================================
// Fetch Flights
// ============================================================================

async function fetchFlightsForDate(dateStr, airlineCode) {
  const dayStart = `${dateStr}T00:00:00Z`;
  const dayEnd = `${dateStr}T23:59:59Z`;

  const allFlights = [];
  let pageNumber = 0;
  let hasMore = true;

  while (hasMore) {
    try {
      const response = await apiRequest('/flightstatus', {
        startRange: dayStart,
        endRange: dayEnd,
        movementType: 'D',
        timeOriginType: 'S',
        timeType: 'U',
        pageSize: CONFIG.pageSize,
        pageNumber,
        operatingAirlineCode: airlineCode,
      });

      const flights = response.operationalFlights || [];
      allFlights.push(...flights);

      const page = response.page || {};
      const totalPages = page.totalPages || 1;
      
      process.stdout.write(`\r   ${dateStr}: Page ${pageNumber + 1}/${totalPages} (${allFlights.length} flights)`);

      hasMore = pageNumber < (totalPages - 1);
      pageNumber++;

      if (pageNumber > 100) break;
    } catch (error) {
      if (error.message.includes('403') || error.message.includes('429')) {
        console.log(`\n   ⚠️  API rate limit reached after ${pageNumber} pages`);
        break;
      }
      throw error;
    }
  }

  process.stdout.write('\n');
  return allFlights;
}

// ============================================================================
// Update Logic
// ============================================================================

function detectChanges(existing, newData, dateStr) {
  const changes = [];

  if (existing.connectivity?.wifi !== newData.connectivity?.wifi) {
    changes.push({
      timestamp: dateStr,
      property: 'connectivity.wifi',
      old_value: existing.connectivity?.wifi,
      new_value: newData.connectivity?.wifi,
      source: 'airline_api',
    });
  }

  if (existing.connectivity?.wifi_provider !== newData.connectivity?.wifi_provider) {
    changes.push({
      timestamp: dateStr,
      property: 'connectivity.wifi_provider',
      old_value: existing.connectivity?.wifi_provider,
      new_value: newData.connectivity?.wifi_provider,
      source: 'airline_api',
    });
  }

  if (existing.cabin?.physical_configuration !== newData.cabin?.physical_configuration) {
    changes.push({
      timestamp: dateStr,
      property: 'cabin.physical_configuration',
      old_value: existing.cabin?.physical_configuration,
      new_value: newData.cabin?.physical_configuration,
      source: 'airline_api',
    });
  }

  if (existing.operator?.sub_fleet_code !== newData.operator?.sub_fleet_code) {
    changes.push({
      timestamp: dateStr,
      property: 'operator.sub_fleet_code',
      old_value: existing.operator?.sub_fleet_code,
      new_value: newData.operator?.sub_fleet_code,
      source: 'airline_api',
    });
  }

  return changes;
}

function mergeAircraft(existing, newData, changes, dateStr) {
  existing.connectivity = newData.connectivity;
  existing.cabin.physical_configuration = newData.cabin.physical_configuration;
  existing.cabin.total_seats = newData.cabin.total_seats;
  existing.cabin.classes = newData.cabin.classes;
  existing.operator = newData.operator;
  existing.aircraft_type = newData.aircraft_type;
  
  existing.tracking.last_seen = dateStr;
  existing.tracking.total_flights = (existing.tracking.total_flights || 0) + 1;
  existing.metadata.updated_at = new Date().toISOString();
  
  if (changes.length > 0) {
    const existingKeys = new Set(
      existing.history.map(h => `${h.timestamp}|${h.property}|${h.old_value}|${h.new_value}`)
    );
    
    for (const change of changes) {
      const key = `${change.timestamp}|${change.property}|${change.old_value}|${change.new_value}`;
      if (!existingKeys.has(key)) {
        existing.history.push(change);
      }
    }
  }
  
  return existing;
}

// ============================================================================
// Main
// ============================================================================

function printHelp() {
  console.log(`
✈️  Air France / KLM Fleet Catalog Updater

Usage:
  node fleet-update.js --airline <CODE> [options]

Required:
  --airline <CODE>    Airline code: AF (Air France) or KL (KLM)

Options:
  --dry-run           Preview changes without saving
  --date <YYYY-MM-DD> Use specific date instead of today
  --bootstrap         Build catalog from scratch (crawl last 7 days)
  --days <N>          Number of days for bootstrap (default: 7)
  --verbose           Show detailed output
  --output-changes    Export changes to changes.json
  --stale-days <N>    Days threshold for stale aircraft (default: 30)
  --help              Show this help message

Environment:
  AFKLM_API_KEY       Single API key
  AFKLM_API_KEYS      Comma-separated API keys (for rotation)

Examples:
  node fleet-update.js --airline AF                  # Update Air France
  node fleet-update.js --airline KL --bootstrap      # Build KLM catalog
  node fleet-update.js --airline KL --dry-run        # Preview KLM changes
`);
}

function getDateRange(startDate, days) {
  const dates = [];
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(startDate);
    d.setDate(d.getDate() - i);
    dates.push(formatDate(d));
  }
  return dates;
}

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    printHelp();
    process.exit(0);
  }
  
  // Parse arguments
  const airlineArg = args.find((_, i) => args[i - 1] === '--airline');
  const dryRun = args.includes('--dry-run');
  const verbose = args.includes('--verbose') || args.includes('-v');
  const outputChanges = args.includes('--output-changes');
  const bootstrap = args.includes('--bootstrap');
  const dateArg = args.find((_, i) => args[i - 1] === '--date');
  const daysArg = args.find((_, i) => args[i - 1] === '--days');
  const staleDaysArg = args.find((_, i) => args[i - 1] === '--stale-days');
  
  const staleDays = parseInt(staleDaysArg || '30', 10);
  const bootstrapDays = parseInt(daysArg || '7', 10);
  
  // Validate airline
  if (!airlineArg || !AIRLINES[airlineArg]) {
    console.error('❌ Error: --airline is required (AF or KL)');
    printHelp();
    process.exit(1);
  }
  
  const airlineCode = airlineArg.toUpperCase();
  const airline = AIRLINES[airlineCode];
  
  // Load API keys from environment
  const apiKeys = (process.env.AFKLM_API_KEYS || process.env.AFKLM_API_KEY || '').split(',').filter(k => k);
  if (apiKeys.length === 0) {
    console.error('❌ Error: No API key found. Set AFKLM_API_KEY or AFKLM_API_KEYS environment variable.');
    process.exit(1);
  }
  
  // Configure
  CONFIG.apiKeys = apiKeys;
  CONFIG.airlineCode = airlineCode;
  CONFIG.catalogPath = path.join(__dirname, 'airlines', `${airlineCode}.json`);
  
  console.log(`\n✈️  ${airline.name} Fleet Catalog Updater\n`);
  console.log(`   🔑 API keys loaded: ${apiKeys.length}`);
  
  if (dryRun) {
    console.log('   🔍 DRY RUN - no changes will be saved\n');
  }

  // Load or create catalog
  let catalog;
  const catalogExists = fs.existsSync(CONFIG.catalogPath);
  
  if (catalogExists && !bootstrap) {
    console.log(`📂 Loading ${CONFIG.catalogPath}...`);
    const content = fs.readFileSync(CONFIG.catalogPath, 'utf-8');
    catalog = JSON.parse(content);
    console.log(`   Found ${catalog.aircraft_count} aircraft\n`);
  } else {
    if (bootstrap) {
      console.log(`🚀 Bootstrap mode: Creating new catalog for ${airline.name}\n`);
    } else {
      console.log(`📂 No existing catalog found, creating new one\n`);
    }
    catalog = {
      schema_version: '1.0.0',
      airline: {
        iata_code: airlineCode,
        name: airline.name,
        country: airline.country,
      },
      generated_at: new Date().toISOString(),
      aircraft_count: 0,
      aircraft: [],
    };
  }

  // Build lookup
  const aircraftByReg = new Map();
  catalog.aircraft.forEach(a => aircraftByReg.set(a.registration, a));

  // Determine dates to process
  let datesToProcess;
  if (bootstrap) {
    datesToProcess = getDateRange(new Date(), bootstrapDays);
    console.log(`📅 Crawling ${bootstrapDays} days: ${datesToProcess[0]} → ${datesToProcess[datesToProcess.length - 1]}\n`);
  } else {
    const targetDate = dateArg || formatDate(new Date());
    datesToProcess = [targetDate];
    console.log(`📅 Processing: ${targetDate}\n`);
  }

  // Process each date
  let totalNew = 0;
  let totalUpdated = 0;
  let totalSeen = 0;
  const allChanges = [];
  const seenAircraftAll = new Map();

  for (const dateStr of datesToProcess) {
    console.log(`📡 Fetching ${airlineCode} flights for ${dateStr}...`);
    
    const flights = await fetchFlightsForDate(dateStr, airlineCode);
    
    // Extract aircraft
    const seenToday = new Map();
    for (const flight of flights) {
      const extracted = extractAircraftFromFlight(flight, airlineCode);
      if (extracted && extracted.registration) {
        seenToday.set(extracted.registration, extracted);
        seenAircraftAll.set(extracted.registration, { data: extracted, date: dateStr });
      }
    }

    console.log(`   ✈️  ${seenToday.size} unique ${airlineCode} aircraft\n`);

    // Process
    for (const [reg, rawData] of seenToday) {
      const newData = transformToSchema(rawData, dateStr);
      const existing = aircraftByReg.get(reg);

      if (!existing) {
        totalNew++;
        if (verbose || bootstrap) {
          console.log(`   ➕ NEW: ${reg} (${rawData.typeName || 'Unknown'})`);
        }
        
        if (!dryRun) {
          catalog.aircraft.push(newData);
          aircraftByReg.set(reg, newData);
        }
      } else {
        const changes = detectChanges(existing, newData, dateStr);
        
        if (changes.length > 0) {
          totalUpdated++;
          if (verbose) {
            console.log(`   🔄 UPDATED: ${reg}`);
            changes.forEach(c => console.log(`      ${c.property}: ${c.old_value} → ${c.new_value}`));
          }
          allChanges.push(...changes.map(c => ({ registration: reg, ...c })));
          
          if (!dryRun) {
            mergeAircraft(existing, newData, changes, dateStr);
          }
        } else {
          totalSeen++;
          if (!dryRun) {
            existing.tracking.last_seen = dateStr;
            existing.tracking.total_flights = (existing.tracking.total_flights || 0) + 1;
          }
        }
      }
    }
  }

  // Summary
  console.log('\n' + '═'.repeat(50));
  console.log('📊 Summary');
  console.log('═'.repeat(50));
  console.log(`   New aircraft:     ${totalNew}`);
  console.log(`   Updated aircraft: ${totalUpdated}`);
  console.log(`   Seen (no change): ${totalSeen}`);
  console.log(`   Total in catalog: ${catalog.aircraft.length}`);
  console.log(`   Total changes:    ${allChanges.length}`);
  console.log(`   API requests:     ${totalRequests}`);

  // Stale aircraft
  if (!bootstrap) {
    const notSeen = catalog.aircraft.filter(a => !seenAircraftAll.has(a.registration));
    const todayDate = new Date();
    const staleThreshold = new Date(todayDate.getTime() - staleDays * 24 * 60 * 60 * 1000);
    const staleAircraft = notSeen.filter(a => {
      if (!a.tracking?.last_seen) return true;
      return new Date(a.tracking.last_seen) < staleThreshold;
    });
    
    if (staleAircraft.length > 0) {
      console.log(`\n⚠️  Stale aircraft (not seen in ${staleDays}+ days): ${staleAircraft.length}`);
      staleAircraft.slice(0, 5).forEach(a => {
        console.log(`   - ${a.registration} (last: ${a.tracking?.last_seen || 'never'})`);
      });
      if (staleAircraft.length > 5) console.log(`   ... and ${staleAircraft.length - 5} more`);
    }
  }

  // WiFi stats
  const wifiStats = { none: 0, 'low-speed': 0, 'high-speed': 0 };
  catalog.aircraft.forEach(a => {
    const wifi = a.connectivity?.wifi || 'none';
    wifiStats[wifi] = (wifiStats[wifi] || 0) + 1;
  });
  const total = catalog.aircraft.length;
  console.log('\n📶 Fleet WiFi Status:');
  console.log(`   High-speed (Starlink): ${wifiStats['high-speed']} (${total ? Math.round(wifiStats['high-speed'] / total * 100) : 0}%)`);
  console.log(`   Low-speed:             ${wifiStats['low-speed']} (${total ? Math.round(wifiStats['low-speed'] / total * 100) : 0}%)`);
  console.log(`   None:                  ${wifiStats['none']} (${total ? Math.round(wifiStats['none'] / total * 100) : 0}%)`);

  // Export changes
  if (outputChanges && allChanges.length > 0) {
    const changesPath = path.join(__dirname, `${airlineCode.toLowerCase()}-changes.json`);
    fs.writeFileSync(changesPath, JSON.stringify({
      generated_at: new Date().toISOString(),
      airline: airlineCode,
      changes: allChanges,
    }, null, 2));
    console.log(`\n📝 Changes exported to ${changesPath}`);
  }

  // Save
  if (!dryRun && (totalNew > 0 || totalUpdated > 0 || totalSeen > 0)) {
    catalog.generated_at = new Date().toISOString();
    catalog.aircraft_count = catalog.aircraft.length;

    catalog.aircraft.sort((a, b) => {
      const typeCompare = (a.aircraft_type?.iata_code || '').localeCompare(b.aircraft_type?.iata_code || '');
      if (typeCompare !== 0) return typeCompare;
      return a.registration.localeCompare(b.registration);
    });

    // Ensure directory exists
    const dir = path.dirname(CONFIG.catalogPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    console.log(`\n💾 Saving to ${CONFIG.catalogPath}...`);
    fs.writeFileSync(CONFIG.catalogPath, JSON.stringify(catalog, null, 2));
    console.log('✅ Done!');
  } else if (dryRun) {
    console.log('\n🔍 Dry run complete - no changes saved');
  } else {
    console.log('\n✅ No changes to save');
  }

  console.log();
}

main().catch(error => {
  console.error(`\n❌ Error: ${error.message}`);
  if (process.env.DEBUG) console.error(error.stack);
  process.exit(1);
});

