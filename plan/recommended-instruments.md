# Recommended instruments for the proposed pilot

Status: **provisional shortlist**, researched **2026-10-18** from official
manufacturer sources. Recheck models/options/manuals and obtain a local quote
at the [M04 decision](hardware-pilot.md). No purchase, connection, scan or
hardware operation is authorized. The budget target is below US$100 total. Local purchasing region and existing
inventory remain unknown. Listed US component prices are separated from explicit
planning allowances; complete landed cost still requires a local quote.

## Primary recommendation: target below US$100 total

Use a small USB-serial microcontroller thermal rig. Commercial SCPI bench
instruments below are **borrowed/future alternatives**, not recommendations to
purchase within this budget. Reuse existing parts first, but the allowance below
includes a supply and a basic electrical check meter; it does not assume either
is owned. An existing development computer is assumed. Assembly tools/labor and
traceable calibration are not funded; borrow tools/operator help or replan.

This is a **US$98.85 target allowance**, not a guaranteed landed quotation.
Four prices are observed on Adafruit's US storefront on 2026-10-18, USD, before
shipping and tax; remaining rows are explicit planning allowances, not researched
vendor offers. Pico H and the reference supply pages displayed unavailable at
retrieval; no immediate orderable complete basket is claimed. Requote local
equivalents after region is known, and do not substitute electrical parts
without engineering review.

| Item | Quantity | USD amount | Evidence / status |
| --- | --- | --- | --- |
| Raspberry Pi Pico H, pre-soldered headers | 1 | 5.00 | [Adafruit 5525](https://www.adafruit.com/product/5525), listed US price; availability must be rechecked. |
| Waterproof DS18B20 probe with included 4.7 kΩ pull-up | 1 | 9.95 | [Adafruit 381](https://www.adafruit.com/product/381), listed US price; supports 3.0–5 V, 1-Wire. |
| MOSFET driver breakout | 1 | 3.95 | [Adafruit 5648](https://www.adafruit.com/product/5648), listed US price; 3–20 V logic input, so 3.3 V control is within documented range. |
| Regulated enclosed 5 V supply | 1 | 7.95 | [Adafruit 276](https://www.adafruit.com/product/276), listed US price for 5 V/2 A reference; regional plug and availability unresolved. This is not an adjustable current-limited lab supply. |
| Small rated power resistor/heater + thermal mass/contact mounting | set | 8.00 | Planning allowance; exact resistance/rating/contact selected by future engineering review. |
| Independent thermal cutoff + fuse/holder + appropriately rated current-limiting protection | set | 10.00 | Planning allowance; actual rated circuit/parts must be reviewed and quoted. Adapter overload protection alone is insufficient. |
| USB data cable, power connector, driver cable, jumpers and secure terminals | set | 9.00 | Planning allowance; includes all connector paths, not only USB. |
| Small prototyping/fixture board, insulated stand/guard | set | 5.00 | Planning allowance; no hot heater on solderless breadboard. |
| Basic low-voltage electrical check meter | 1 | 10.00 | Planning allowance, not a calibrated temperature reference or a mains-rated recommendation; use only reviewed low-energy DC checks. |
| Shipping/tax/region-adapter contingency | total | 30.00 | Planning allowance, not a confirmed delivery/tax calculation. |
| **Target total** | | **98.85** | GO only if actual complete landed quote fits the user's approved ceiling. |

The engineering gate may reveal protection/fixture costs above these allowances.
In that case defer the heated pilot, borrow suitable equipment, or propose a
passive low-voltage RC experiment with its own reviewed scope. Do not remove the
cutoff, protection, supply, meter or shipping allowance to pretend the kit fits.
The default simulated eight-point thermal model remains unchanged.

### Compatibility and software work still required

Pico GPIO is a 3.3 V domain: power the DS18B20 and its data pull-up at 3.3 V;
never pull a GPIO input up to the heater's 5 V rail. Sensor product documentation
supports that supply range; driver product documentation supports a 3.3 V command.
The heater receives switched supply power through the driver, never from a GPIO.
The manufacturer rates this driver for **1.5 A continuous**; its higher peak
rating is not a continuous heater rating ([driver source](https://www.adafruit.com/product/5648),
retrieved 2026-10-18). Do not transfer the simulated **10 W** command to the 5 V
rig: it would demand **2 A**, beyond that continuous rating. The actual physical
run/model configuration and a lower conservative current/power limit require
separate engineering review of heater, driver, supply, wiring and cutoff. No
component maximum is a recommended operating point.
Exact grounding, GPIO boot-state pull-down, fuse/current protection, cutoff
contact rating and heater rating need a schematic/design review before assembly.
No pin-level construction instructions or validated electrical design are provided.
Use [Raspberry Pi Pico documentation](https://www.raspberrypi.com/documentation/microcontrollers/pico-series.html)
for board/power/USB details (retrieved 2026-10-18); verify exact board revision.

A slow thermal experiment fits this sensor better than a fast circuit transient:
its documented conversion can take approximately 750 ms. Respect conversion-ready
and sensor validity/CRC semantics; report actual sample time and sensor/contact
lag. The DS18B20 is not a traceably calibrated reference thermometer.

M04 must estimate and review **additional firmware plus custom USB-serial adapter
work**, provisionally 1–2 working weeks beyond example integration, including
protocol version/sequence IDs, command acknowledgement, stale/invalid samples,
startup heater-off, local watchdog/off-on-disconnect and sensor-fault handling.
The host and firmware clocks must be distinguished. Independent cutoff remains
necessary even with watchdog firmware. This is not SCPI/PyVISA compatibility.
Any host serial library (e.g. pyserial) requires a separate scoped dependency
choice, preferably optional/example-specific; this plan adds no core dependency.
M05 pilot follows accepted firmware/adapter tests; move scan scope/dates if needed.

## Optional borrowed or future bench instruments

For existing lab equipment, use one remotely programmable low-voltage
current-limited supply and a meter/DAQ with an explicitly supported temperature
sensor. The following models are documented alternatives only; **all require
local quotations and are outside the proposed US$100 purchase kit**. No price or
availability claim is made for them. Avoid buying an oscilloscope/generator solely
for the thermal pilot.

| Tier / role | Exact model or item | Verified capability and control path | Tradeoff / decision |
| --- | --- | --- | --- |
| Minimal supply | **RIGOL DP832** | Three outputs: two 30 V/3 A and one 5 V/3 A; USB host/device standard. LAN/RS232 are optional for this model; DP832A includes them. DP800 programming guide is available from official downloads. [Datasheet](https://download.rigol.com/en/Manual/DC%20Power%26Loads/DP800/DP800_DataSheet_EN.pdf), [manuals/programming guides](https://www.rigolna.com/support/downloads/). | Use one output well below its rating with a reviewed current/power limit; maximum instrument rating is not pilot operating power. USB avoids assuming a LAN option. Quote DP832 and required cables/options explicitly. |
| Minimal temperature channel | **Keysight EDU34450A + E2308A probe** | DMM temperature conversion specifically supports a **5 kΩ thermistor**; the user guide identifies E2308A. USB and LAN with VISA resource formats are documented, and the data sheet describes SCPI control. [User guide](https://www.keysight.com/us/en/assets/9921-01392/user-manuals/EDU34450A-Digital-Multimeter-Users-Guide.pdf), [data sheet](https://www.keysight.com/zz/en/assets/3121-1002/data-sheets/EDU34450A-5-5-Digit-Dual-Display-Digital-Multimeter.pdf). | This is **not a thermocouple recommendation**. Include the probe in the quote; do not assume it is bundled. Use the manufacturer's supported conversion, not any arbitrary 5 kΩ curve. |
| Standard supply alternative | **Keysight E36312A** | 80 W triple output; 6 V/5 A plus two 25 V/1 A channels; LAN and USB. E36300 programming guide documents SCPI. [Exact product](https://www.keysight.com/us/en/product/E36312A/80w-triple-output-power-supply-6v-5a-2x-25v-1a.html), [programming guide](https://www.keysight.com/cg/en/assets/9018-04577/programming-guides/9018-04577.pdf). | Native LAN is convenient; extra channels/power may exceed the pilot need. Confirm regional mains configuration. Quote rather than assuming a price advantage. |
| Flexible meter alternative | **Keithley DMM6500** | Temperature supports thermistors, 2/3/4-wire RTDs and thermocouples; SCPI reference documents sensor selection. USB/LAN operation is documented in the data sheet. [Data sheet](https://download.tek.com/datasheet/1KW-61315-0_DMM6500_Datasheet_031618.pdf), [SCPI reference](https://download.tek.com/manual/DMM6500-901-01_A_April_2018_Ref_DMM6500-901-01A.pdf), [current support/revisions](https://www.tek.com/en/support/datasheets-manuals-software-downloads?model=DMM6500). | Prefer a compatible 4-wire RTD if accurate contact sensing is the objective and fixture/wiring warrant it. Thermocouple use requires a reviewed reference-junction solution; do not assume internal cold-junction compensation or a scanner card is bundled. |
| Expanded multichannel option | **Keysight DAQ970A + DAQM901A** | USB/LAN mainframe with built-in DMM; DAQM901A 20-channel multiplexer. Family supports thermocouples, RTDs and thermistors; SCPI programming guide is listed in support. [Technical overview](https://www.keysight.com/content/dam/keysight/en/doc/ungate/technical-overviews/5992-3168.pdf), [module catalog](https://go.keysight.com/us/en/assets/7120-1212/catalogs/Data-Acquisition-Switch-Solutions-Catalog.pdf), [programming resources](https://www.keysight.com/us/en/support/DAQ973A/data-acquisition-system-usb-lan-gpib.html). | Buy only if multiple body/ambient sensors or later scanning justify the mainframe and module cost. Mainframe alone is not a wired multichannel temperature kit. Confirm channel wiring, cold-junction setup and scan timestamps against the exact module/manual. |

All table sources were retrieved 2026-10-18; older dated manuals establish
specific contracts, not latest firmware qualification. Use the manufacturer's
support page to select the revision matching the actual unit. Mention of USB/LAN
and SCPI supports a **proposed PyVISA integration path**; no host/backend/device
combination has been connected or tested in this planning task. Native vendor
GUI compatibility does not prove Python/macOS/Linux compatibility.

## Probe, fixture and accessories are part of the kit

E2308A is a stainless-encapsulated thermistor, 5000 Ω at 25 °C, with dual-banana
termination. Its published liquid response time does not establish the response
of a surface-mounted thermal fixture. Clamp/contact and probe lag must be
characterized; check fit before choosing it for a small block.
[Official E2307A/E2308A guide](https://www.keysight.com/us/en/assets/9018-01162/user-manuals/9018-01162.pdf)
(retrieved 2026-10-18). If a thinner surface sensor is needed, choose an RTD or
thermistor with a documented curve that the selected meter explicitly supports;
select the exact probe after fixture design, rather than substituting by nominal
resistance alone.

Include a power resistor/heater with documented rating and mounting, small metal
mass, suitable electrically insulating thermal contact where needed, secure
stand/enclosure, rated wiring/connectors, current protection, independent thermal
cutoff and accessible manual output disable. An operator reviews conservative
voltage/current/power/temperature limits before operation. Keep the first pilot
small and attended; no mains-connected DUT or exposed high-temperature assembly.
The software is not the independent cutoff. Purchase exact fixture parts only
after the mechanical/electrical arrangement is reviewed.

For DMM6500 thermocouples, the specification describes external or simulated
reference junctions; explicitly budget a suitable junction measurement/scanner
solution if that sensor route is selected. A fixed simulated junction is not an
automatic compensation measurement.
[Official specifications](https://download.tek.com/document/SPEC-DMM6500A_April_2018.pdf)
(retrieved 2026-10-18). A 4-wire RTD can avoid that particular junction issue but
adds wiring and probe-selection requirements.

## Optional later circuit instruments

These serve the M09 second-domain case only after its bandwidth/timing needs
are chosen. Reuse existing equipment first; they are not M04/M05 prerequisites.

- **RIGOL DHO804** oscilloscope: exact 70 MHz/four-channel model is listed by the
  manufacturer's store; DHO800 series documents USB/LAN remote control. Review
  DHO800/DHO900 programming commands and waveform transfer against the unit.
  [Official exact model](https://mall.rigol.com/shiboqi/dho804.html),
  [series interfaces](https://www.rigolna.com/europe/products/oscilloscope/DHO800.html),
  [programming downloads](https://www.rigolna.com/support/downloads/).
- **RIGOL DG821 Pro** function generator: single channel, 25 MHz in the official
  DG800 Pro/DG900 Pro programming guide, which documents SCPI over USB/LAN.
  One channel is enough for a simple RC response; use an owned generator instead
  if suitable. Do not confuse DG800 and DG800 **Pro** interface/options.
  [Official programming guide](https://www.rigol.com/dam/global/downloads/brochures/en/program-guide/waveform-generators/DG800ProDG900Pro_ProgrammingGuide_EN.pdf).

Sources retrieved 2026-10-18. No pricing, availability or software-driver
qualification is inferred. Check oscilloscope/probe ground connections in the
reviewed low-voltage circuit; no mains or high-energy probing is in this pilot.

## M04 procurement and compatibility checklist

Before a purchase recommendation becomes a named pilot setup, record:

1. Owned/borrowed inventory, owner consent, condition/calibration history and
   whether the task needs a new instrument at all.
2. Exact SKU/region/options, probe model/curve/range/connector, required scanner
   or reference-junction accessories, output limits and fixture contact design.
3. Manual/programming-guide revision; firmware/protocol; selected commands; host
   OS, Python and USB-serial backend/library/version/port identity for the budget
   rig. Record VISA implementation, PyVISA version and USB/LAN resource type only
   for the optional bench-instrument path. Include required cables.
   Verify this combination in the separately authorized pilot, not by assumption.
4. Itemized local quotation in the user's chosen currency/region with tax,
   delivery, warranty, calibration, accessories and total spending ceiling.
   The four central component amounts are observed manufacturer US storefront
   list prices; support parts and contingency are planning allowances. Neither
   category establishes a complete landed quotation; confirm the full kit below
   the US$100 target before GO.
5. Access dates/operator, data rights/storage, independent cutoff and output-off
   verification, pre-agreed acceptance thresholds and a GO/DEFER/NO-GO decision.

The recommendation should be revisited when inventory and purchasing region are known and the
complete landed quote has been checked against the below-US$100 target. A lower purchase price that adds undocumented protocols,
unsupported probes or extra adapter work can cost more project time; compare the
complete executable setup rather than instrument headline ratings.
