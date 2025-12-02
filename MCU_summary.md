CMD = COMMAND PORT 
CTRL = CONTROL PORT
STATUS = STATUS PORT
  
  
control_port: 2002
command_port: 2000
status_port: 2001




| Code     | Name / Function                     | Notes / Behavior                                                                                                                                     | Port                                                                                     |         |
| -------- | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------- |
| **G1**   | Linear move                         | Moves X and/or Z axes (ZA + ZB) with optional `F` (mm/min) feedrate and `A` (mm/min²) accel. Checks enable, faults, and program state before motion. | **CMD**                                                                                  |         |
| **G10**  | Dual-Z move / independent Z control | Moves ZA and/or ZB individually (or both via `Z`). Accepts `F` and `A`.                                                                              | **CMD**                                                                                  |         |
| **G28**  | Home axes                           | Homes X and/or Z (ZA/ZB). `F` and `A` set approach speeds/accel. Sets `g_homed` flags true on success.                                               | **CMD**                                                                                  |         |
| **G60**  | Guarded move on X                   | `G60 X.. P.. [S0 S1] [T..] [F..] [A..]`. Moves until pin trigger or timeout. Deferred OKAY/FAULT via FSM.                                            | **CMD**                                                                                  |         |
| **G92**  | Set axis position (zero offset)     | Sets current axis positions (X/Z/ZA/ZB) without motion. Optionally marks axes homed.                                                                 | **CMD**                                                                                  |         |
| **G920** | Set position & **clear home**       | Same syntax as G92 but clears `g_homed` flags for the axes specified.                                                                                | **CMD**                                                                                  |         |


| Code     | Name / Function      | Notes / Behavior                                                                                                                     | Port       |
| -------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| **M17**  | Enable motors        | Enables listed axes (`M17 X ZA ZB`) or all if none given.                                                                            | **CMD**    |
| **M18**  | Disable motors       | Disables listed axes (or all if none). Clears their homed flags.                                                                     | **CMD**    |
| **M24**  | Resume motion        | Resumes paused motion or guarded move. Uses per-axis saved targets.                                                                  | **CTRL**   |
| **M25**  | Pause (feed hold)    | Pauses running motion; saves per-axis targets for M24 resume.                                                                        | **CTRL**   |
| **M2**   | Abort / stop program | Emergency stop: halts all motion, sets `programState = ABORTED`, clears saved targets.                                               | **CTRL**   |
| **M100** | Clear ABORTED state  | Valid only if `programState == ABORTED` and all ports connected. Sets state → IDLE.                                                  | **CTRL**   |
| **M101** | Clear saved targets  | Clears saved targets selectively (`M101 X`, `M101 ZA`, etc.) when paused.                                                            | **CTRL**   |
| **M123** | Status report        | Returns JSON snapshot: last 3 commands, program state, motor enable/fault/homed flags, positions, targets, remaining, speed & accel. | **STATUS** |




| MCU Architecture     | Purpose                                                                       |
| -------------------- | ----------------------------------------------------------------------------- |
| **Program states**   | `IDLE`, `RUNNING`, `PAUSED`, `ABORTED`  — mutex-protected under `stateMutex`. |
| **Guarded move FSM** | Handles G60 triggers and timeouts with deferred OKAY/FAULT.                   |
| **Saved targets**    | `g_saved.{X,ZA,ZB}` store positions for M25/M24 pause/resume per-axis.        |
| **Homed flags**      | `g_homed.{X,ZA,ZB}` track successful homing; cleared by M18 or G920.          |
| **Fault monitor**    | Event-driven FreeRTOS task wakes on ISR notify for X/ZA/ZB faults.            |
