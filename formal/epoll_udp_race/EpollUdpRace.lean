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
  deriving DecidableEq, Repr

inductive Event where
  | readable
  | activate
  | progress
  | deadline (generation : Nat)
  | explicitClose
  deriving DecidableEq, Repr

def initial : Model := {
  phase := .preActive
  dataPending := false
  readReady := false
  generation := 1
  terminal := none
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

def unsafeStep (s : Model) (event : Event) : Model :=
  match event with
  | .readable => unsafeReadable s
  | .activate => activate s
  | .progress => progress s
  | .deadline generation => unsafeDeadline generation s
  | .explicitClose => unsafePublishTerminal .explicitClose s

def safeStep (s : Model) (event : Event) : Model :=
  match event with
  | .readable => safeReadable s
  | .activate => activate s
  | .progress => progress s
  | .deadline generation => safeDeadline generation s
  | .explicitClose => safePublishTerminal .explicitClose s

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
  decide

theorem safe_preActive_readable_edge_is_preserved :
    ¬ stuckReadable (run safeStep initial preActiveReadableTrace) := by
  decide

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
  decide

theorem safe_stale_deadline_is_ignored :
    (run safeStep initial staleDeadlineTrace).terminal = none ∧
    (run safeStep initial staleDeadlineTrace).phase = .active ∧
    (run safeStep initial staleDeadlineTrace).generation = 2 := by
  decide

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
  decide

theorem safe_explicit_close_remains_first_terminal_owner :
    (run safeStep initial closeThenTimeoutTrace).terminal = some .explicitClose := by
  decide

theorem safe_first_terminal_owner
    (s : Model) (first second : Terminal)
    (h : s.terminal = none) :
    (safePublishTerminal second (safePublishTerminal first s)).terminal = some first := by
  simp [safePublishTerminal, h]

end Ogrenet.EpollUdpRace
