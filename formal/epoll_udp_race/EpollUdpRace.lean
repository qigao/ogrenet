namespace Ogrenet.EpollUdpRace

inductive Phase where
  | preActive
  | active
  | closed
  deriving DecidableEq, Repr

inductive Terminal where
  | explicitClose
  | timeout
  deriving DecidableEq, Repr

structure Model where
  phase : Phase
  dataPending : Bool
  readReady : Bool
  generation : Nat
  terminal : Option Terminal
  draining : Bool
  admittedPacket : Bool
  packetWritten : Bool
  deriving DecidableEq, Repr

inductive Event where
  | readable
  | activate
  | progress
  | deadline (generation : Nat)
  | explicitClose
  | admitPacket
  | beginDrain
  | physicalWrite
  deriving DecidableEq, Repr

def initial : Model := {
  phase := .preActive
  dataPending := false
  readReady := false
  generation := 1
  terminal := none
  draining := false
  admittedPacket := false
  packetWritten := false
}

def activate (s : Model) : Model :=
  match s.phase with
  | .preActive => { s with phase := .active }
  | _ => s

/-- Unsafe ET semantics: data may arrive before Active, but the edge itself is
    discarded and no readiness ownership is retained. -/
def unsafeReadable (s : Model) : Model :=
  match s.phase with
  | .preActive => { s with dataPending := true }
  | .active => { s with dataPending := true, readReady := true }
  | .closed => s

/-- Safe ET semantics: a pre-active readable edge is retained as reactor-owned
    readiness and consumed only after activation. -/
def safeReadable (s : Model) : Model :=
  match s.phase with
  | .closed => s
  | _ => { s with dataPending := true, readReady := true }

def progress (s : Model) : Model :=
  { s with generation := s.generation + 1 }

/-- Unsafe deadline semantics: an old timeout publication can close the resource
    regardless of progress generation or an already-published terminal owner. -/
def unsafeDeadline (_observedGeneration : Nat) (s : Model) : Model :=
  { s with phase := .closed, terminal := some .timeout }

/-- Safe deadline semantics: only the current generation of a live resource may
    publish the timeout terminal owner. -/
def safeDeadline (observedGeneration : Nat) (s : Model) : Model :=
  if s.phase = .active ∧ observedGeneration = s.generation ∧ s.terminal = none then
    { s with phase := .closed, terminal := some .timeout }
  else
    s

def unsafePublishTerminal (terminal : Terminal) (s : Model) : Model :=
  { s with phase := .closed, terminal := some terminal }

/-- First-terminal-owner arbitration. Once a terminal owner exists, later
    publishers are no-ops. -/
def safePublishTerminal (terminal : Terminal) (s : Model) : Model :=
  match s.terminal with
  | none => { s with phase := .closed, terminal := some terminal }
  | some _ => s

/-- Packet admission transfers ownership to the runtime only while the resource
    is Active and has not entered Engine drain. -/
def admitPacket (s : Model) : Model :=
  match s.phase with
  | .active => if s.draining then s else { s with admittedPacket := true }
  | _ => s

/-- Unsafe Engine drain semantics: beginning drain closes the PacketConn
    immediately even when one already-admitted datagram is still runtime-owned. -/
def unsafeBeginDrain (s : Model) : Model :=
  match s.phase with
  | .active => { s with draining := true, phase := .closed }
  | _ => s

/-- Safe Engine drain semantics: new admission is closed immediately, but a
    PacketConn with retained datagram ownership stays Active until physical
    completion releases that ownership. -/
def safeBeginDrain (s : Model) : Model :=
  match s.phase with
  | .active =>
      if s.admittedPacket then
        { s with draining := true }
      else
        { s with draining := true, phase := .closed }
  | _ => s

/-- A closed unsafe resource cannot physically write the datagram it discarded
    during premature drain finalization. -/
def unsafePhysicalWrite (s : Model) : Model :=
  match s.phase with
  | .active =>
      if s.admittedPacket then
        { s with admittedPacket := false, packetWritten := true }
      else
        s
  | _ => s

/-- Safe physical completion is the ownership-release point. During Engine
    drain, that completion is also the barrier that permits clean close. -/
def safePhysicalWrite (s : Model) : Model :=
  match s.phase with
  | .active =>
      if s.admittedPacket then
        if s.draining then
          { s with admittedPacket := false, packetWritten := true, phase := .closed }
        else
          { s with admittedPacket := false, packetWritten := true }
      else
        s
  | _ => s

def unsafeStep (s : Model) (event : Event) : Model :=
  match event with
  | .readable => unsafeReadable s
  | .activate => activate s
  | .progress => progress s
  | .deadline generation => unsafeDeadline generation s
  | .explicitClose => unsafePublishTerminal .explicitClose s
  | .admitPacket => admitPacket s
  | .beginDrain => unsafeBeginDrain s
  | .physicalWrite => unsafePhysicalWrite s

def safeStep (s : Model) (event : Event) : Model :=
  match event with
  | .readable => safeReadable s
  | .activate => activate s
  | .progress => progress s
  | .deadline generation => safeDeadline generation s
  | .explicitClose => safePublishTerminal .explicitClose s
  | .admitPacket => admitPacket s
  | .beginDrain => safeBeginDrain s
  | .physicalWrite => safePhysicalWrite s

def run (step : Model → Event → Model) : Model → List Event → Model
  | state, [] => state
  | state, event :: rest => run step (step state event) rest

def stuckReadable (s : Model) : Prop :=
  s.phase = .active ∧
  s.dataPending = true ∧
  s.readReady = false ∧
  s.terminal = none

/-- Real epoll race shape: EPOLLIN arrives before Active; the unsafe transition
    consumes the edge but leaves kernel data pending. After activation there is
    no local readiness owner and ET need not deliver another edge. -/
def preActiveReadableTrace : List Event := [
  .readable,
  .activate
]

theorem unsafe_preActive_readable_edge_has_stuck_witness :
    stuckReadable (run unsafeStep initial preActiveReadableTrace) := by
  simp [stuckReadable, preActiveReadableTrace, run, unsafeStep, unsafeReadable, activate, initial]

theorem safe_preActive_readable_edge_is_preserved :
    ¬ stuckReadable (run safeStep initial preActiveReadableTrace) := by
  simp [stuckReadable, preActiveReadableTrace, run, safeStep, safeReadable, activate, initial]

theorem unsafe_model_has_reachable_readiness_race :
    ∃ trace : List Event, stuckReadable (run unsafeStep initial trace) := by
  exact ⟨preActiveReadableTrace, unsafe_preActive_readable_edge_has_stuck_witness⟩

/-- Real runtime-deadline race shape: generation 1 is armed, real progress moves
    the resource to generation 2, then the stale generation-1 heap entry fires. -/
def staleDeadlineTrace : List Event := [
  .activate,
  .progress,
  .deadline 1
]

theorem unsafe_stale_deadline_can_close_new_generation :
    (run unsafeStep initial staleDeadlineTrace).terminal = some .timeout := by
  simp [staleDeadlineTrace, run, unsafeStep, unsafeDeadline, progress, activate, initial]

theorem safe_stale_deadline_is_ignored :
    (run safeStep initial staleDeadlineTrace).terminal = none ∧
    (run safeStep initial staleDeadlineTrace).phase = .active ∧
    (run safeStep initial staleDeadlineTrace).generation = 2 := by
  simp [staleDeadlineTrace, run, safeStep, safeDeadline, progress, activate, initial]

theorem safe_stale_generation_preserves
    (s : Model) (observedGeneration : Nat)
    (h : observedGeneration ≠ s.generation) :
    safeDeadline observedGeneration s = s := by
  simp [safeDeadline, h]

/-- First-terminal-owner race shape: explicit Close publishes first, then a stale
    timeout publisher runs later. The unsafe model overwrites the first owner. -/
def closeThenTimeoutTrace : List Event := [
  .activate,
  .explicitClose,
  .deadline 1
]

theorem unsafe_timeout_can_overwrite_explicit_close :
    (run unsafeStep initial closeThenTimeoutTrace).terminal = some .timeout := by
  simp [closeThenTimeoutTrace, run, unsafeStep, unsafeDeadline, unsafePublishTerminal, activate, initial]

theorem safe_explicit_close_remains_first_terminal_owner :
    (run safeStep initial closeThenTimeoutTrace).terminal = some .explicitClose := by
  simp [closeThenTimeoutTrace, run, safeStep, safeDeadline, safePublishTerminal, activate, initial]

theorem safe_first_terminal_owner
    (s : Model) (first second : Terminal)
    (h : s.terminal = none) :
    (safePublishTerminal second (safePublishTerminal first s)).terminal = some first := by
  simp [safePublishTerminal, h]

/-- Race observed by the broad Go gate: a datagram is admitted while the owning
    reactor is blocked, Engine drain begins, and unsafe shutdown finalizes the
    PacketConn before that retained datagram reaches physical write. -/
def drainWithAdmittedPacketTrace : List Event := [
  .activate,
  .admitPacket,
  .beginDrain
]

def prematureDrainClose (s : Model) : Prop :=
  s.draining = true ∧
  s.phase = .closed ∧
  s.admittedPacket = true ∧
  s.packetWritten = false

theorem unsafe_engine_drain_can_close_with_admitted_packet :
    prematureDrainClose (run unsafeStep initial drainWithAdmittedPacketTrace) := by
  simp [prematureDrainClose, drainWithAdmittedPacketTrace, run, unsafeStep,
    admitPacket, unsafeBeginDrain, activate, initial]

theorem safe_engine_drain_retains_admitted_packet_until_write :
    (run safeStep initial drainWithAdmittedPacketTrace).phase = .active ∧
    (run safeStep initial drainWithAdmittedPacketTrace).draining = true ∧
    (run safeStep initial drainWithAdmittedPacketTrace).admittedPacket = true ∧
    (run safeStep initial drainWithAdmittedPacketTrace).packetWritten = false := by
  simp [drainWithAdmittedPacketTrace, run, safeStep, admitPacket, safeBeginDrain, activate, initial]

def drainThenWriteTrace : List Event :=
  drainWithAdmittedPacketTrace ++ [.physicalWrite]

theorem safe_engine_drain_closes_only_after_physical_write :
    (run safeStep initial drainThenWriteTrace).phase = .closed ∧
    (run safeStep initial drainThenWriteTrace).draining = true ∧
    (run safeStep initial drainThenWriteTrace).admittedPacket = false ∧
    (run safeStep initial drainThenWriteTrace).packetWritten = true := by
  simp [drainThenWriteTrace, drainWithAdmittedPacketTrace, run, safeStep,
    admitPacket, safeBeginDrain, safePhysicalWrite, activate, initial]

theorem safe_begin_drain_preserves_retained_ownership
    (s : Model)
    (hphase : s.phase = .active)
    (hadmitted : s.admittedPacket = true) :
    (safeBeginDrain s).phase = .active ∧
    (safeBeginDrain s).draining = true ∧
    (safeBeginDrain s).admittedPacket = true := by
  simp [safeBeginDrain, hphase, hadmitted]

end Ogrenet.EpollUdpRace
