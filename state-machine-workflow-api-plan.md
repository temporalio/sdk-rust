# State-Machine Workflow API Design

## Summary

Use synchronous routed methods over serializable workflow state. Macros generate the internal dispatcher, but every result-producing command receives an explicit typed handler reference. Activities, children, and timers therefore cannot be started without declaring how their later events will be handled.

Machine workflows require exactly one `#[init]`. `#[run]` and all async handlers are illegal because no main routine exists.

## Complete Authoring Example

```rust
// Existing typed definitions:
//
// Payments::charge:
//     ChargeRequest -> PaymentReceipt
//
// AddressActivities::validate:
//     Address -> Address
//
// ShippingWorkflow::workflow:
//     ShippingRequest -> Shipment

#[workflow]
#[derive(Serialize, Deserialize)]
struct Checkout {
    order_id: String,
    address: Address,
    gift: bool,

    payment: Option<ActivityRef<PaymentReceipt>>,
    payment_timeout: Option<TimerRef>,
    payment_receipt: Option<PaymentReceipt>,

    address_check: Option<ActivityRef<Address>>,
    pending_address_update: Option<UpdateRef<Address>>,

    shipping: Option<ChildRef<Shipment>>,
    shipping_run_id: Option<String>,
}

#[workflow_methods(
    machine,
    output = Receipt,
    auto_continue = "when_suggested"
)]
impl Checkout {
    #[init]
    fn init(
        ctx: &mut MachineContext<Self>,
        order: Order,
    ) -> MachineResult<Self> {
        let payment = ctx.start_activity(
            Payments::charge,
            ChargeRequest {
                order_id: order.order_id.clone(),
            },
            ActivityOptions::start_to_close_timeout(Duration::from_secs(30)),
            // This synchronous method is called when the activity resolves.
            Self::payment_finished,
        )?;

        let payment_timeout = ctx.start_timer(
            TimerId::new(format!("payment-timeout:{}", order.order_id)),
            TimerOptions::from(Duration::from_secs(60)),
            // This synchronous method is called when the timer fires or is cancelled.
            Self::payment_timeout_elapsed,
        )?;

        Ok(Self {
            order_id: order.order_id,
            address: order.address,
            gift: false,
            payment: Some(payment),
            payment_timeout: Some(payment_timeout),
            payment_receipt: None,
            address_check: None,
            pending_address_update: None,
            shipping: None,
            shipping_run_id: None,
        })
    }

    #[signal]
    fn set_gift(
        &mut self,
        _ctx: &mut MachineContext<Self>,
        gift: bool,
    ) -> MachineResult<()> {
        self.gift = gift;
        Ok(())
    }

    // Successful return accepts and completes the Update immediately.
    #[update]
    fn current_address(
        &mut self,
        _ctx: &mut MachineContext<Self>,
        address: Address,
    ) -> MachineUpdateResult<Address> {
        self.address = address;
        Ok(self.address.clone())
    }

    // Returning DeferredUpdate<Address> accepts without completing.
    #[update]
    fn validate_address(
        &mut self,
        ctx: &mut MachineContext<Self>,
        candidate: Address,
    ) -> MachineUpdateResult<DeferredUpdate<Address>> {
        let check = ctx.start_activity(
            AddressActivities::validate,
            candidate,
            ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            // This synchronous method is called when validation resolves.
            Self::address_validated,
        )?;
        let deferred = ctx.defer_update();

        self.address_check = Some(check);
        self.pending_address_update = Some(deferred.reference());

        Ok(deferred)
    }

    #[activity_result]
    fn address_validated(
        &mut self,
        ctx: &mut MachineContext<Self>,
        operation: ActivityRef<Address>,
        result: Result<Address, ActivityExecutionError>,
    ) -> MachineResult<()> {
        if self.address_check.as_ref() != Some(&operation) {
            return Ok(());
        }
        self.address_check = None;

        let update = self
            .pending_address_update
            .take()
            .expect("address validation must have an associated Update");

        match result {
            Ok(address) => {
                self.address = address.clone();
                ctx.complete_update(update, address)?;
            }
            Err(error) => ctx.fail_update(update, error)?,
        }

        Ok(())
    }

    #[activity_result]
    fn payment_finished(
        &mut self,
        ctx: &mut MachineContext<Self>,
        operation: ActivityRef<PaymentReceipt>,
        result: Result<PaymentReceipt, ActivityExecutionError>,
    ) -> MachineResult<()> {
        if self.payment.as_ref() != Some(&operation) {
            return Ok(());
        }
        self.payment = None;

        if let Some(timeout) = self.payment_timeout.take() {
            ctx.cancel_timer(timeout)?;
        }

        let payment = match result {
            Ok(payment) => payment,
            Err(error) => return ctx.fail_workflow(error),
        };
        self.payment_receipt = Some(payment);

        let shipping = ctx.start_child_workflow(
            ShippingWorkflow::workflow,
            ShippingRequest {
                order_id: self.order_id.clone(),
                address: self.address.clone(),
            },
            ChildWorkflowOptions::workflow_id(format!("{}-shipping", self.order_id)),
            // This synchronous method receives the child's start and completion events.
            Self::shipping_event,
        )?;
        self.shipping = Some(shipping);

        Ok(())
    }

    #[timer]
    fn payment_timeout_elapsed(
        &mut self,
        ctx: &mut MachineContext<Self>,
        timer: TimerRef,
        result: TimerResult,
    ) -> MachineResult<()> {
        if self.payment_timeout.as_ref() != Some(&timer) {
            return Ok(());
        }
        self.payment_timeout = None;

        match result {
            TimerResult::Fired => ctx.fail_workflow(
                ApplicationFailure::non_retryable("payment timed out"),
            ),
            TimerResult::Cancelled => Ok(()),
        }
    }

    #[child_event]
    fn shipping_event(
        &mut self,
        ctx: &mut MachineContext<Self>,
        child: ChildRef<Shipment>,
        event: ChildWorkflowEvent<Shipment>,
    ) -> MachineResult<()> {
        if self.shipping.as_ref() != Some(&child) {
            return Ok(());
        }

        match event {
            ChildWorkflowEvent::Started { run_id } => {
                self.shipping_run_id = Some(run_id);
                Ok(())
            }
            ChildWorkflowEvent::StartFailed(error) => ctx.fail_workflow(error),
            ChildWorkflowEvent::Completed(Err(error)) => ctx.fail_workflow(error),
            ChildWorkflowEvent::Completed(Ok(shipment)) => {
                self.shipping = None;
                let payment = self
                    .payment_receipt
                    .take()
                    .expect("shipping starts only after payment");

                ctx.complete_workflow(Receipt {
                    order_id: self.order_id.clone(),
                    payment_confirmation: payment.confirmation,
                    shipment_id: shipment.shipment_id,
                    gift: self.gift,
                })
            }
        }
    }

    #[query]
    fn status(&self, _ctx: &WorkflowContextView) -> CheckoutStatus {
        CheckoutStatus {
            payment_finished: self.payment_receipt.is_some(),
            shipping_started: self.shipping.is_some(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    address: Address,
}

#[derive(Clone, Serialize, Deserialize)]
struct ChargeRequest {
    order_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct PaymentReceipt {
    confirmation: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct ShippingRequest {
    order_id: String,
    address: Address,
}

#[derive(Clone, Serialize, Deserialize)]
struct Shipment {
    shipment_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    payment_confirmation: String,
    shipment_id: String,
    gift: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct CheckoutStatus {
    payment_finished: bool,
    shipping_started: bool,
}
```

## Typed Handler References

`#[activity_result]`, `#[child_event]`, and `#[timer]` generate zero-sized handler markers. Expressions such as `Self::payment_finished` refer to these markers rather than serializable function pointers.

```rust
fn start_activity<AD, H>(
    &mut self,
    activity: AD,
    input: impl Into<AD::Input>,
    options: ActivityOptions,
    handler: H,
) -> MachineResult<ActivityRef<AD::Output>>
where
    AD: ActivityDefinition,
    H: ActivityResultHandler<Self::Workflow, AD::Output>;

fn start_child_workflow<WD, H>(
    &mut self,
    workflow: WD,
    input: impl Into<WD::Input>,
    options: ChildWorkflowOptions,
    handler: H,
) -> MachineResult<ChildRef<WD::Output>>
where
    WD: WorkflowDefinition,
    H: ChildEventHandler<Self::Workflow, WD::Output>;

fn start_timer<H>(
    &mut self,
    id: impl Into<TimerId>,
    options: impl Into<TimerOptions>,
    handler: H,
) -> MachineResult<TimerRef>
where
    H: TimerResultHandler<Self::Workflow>;
```

- A timer requires a user-defined `TimerId` and a result handler.
- `TimerId` is a serializable string newtype with conversions from `String` and `&str`.
- `TimerRef` contains the user ID plus the SDK's durable operation identity and exposes `id()`.
- Active timer IDs must be unique. Starting a duplicate returns a typed `DuplicateTimerId` error; an ID may be reused after its prior timer resolves.
- Timer handlers receive the existing `TimerResult::{Fired, Cancelled}`.
- There is no overload for starting result-producing work without a handler.
- Explicit SDK markers such as `IgnoreActivityResult`, `IgnoreChildEvents`, and `IgnoreTimerResult` support intentional fire-and-forget behavior.
- Generated handler markers have a stable routing name, defaulting to the method name. Optional `name = "..."` attributes preserve routing across method renames.

The same handler-reference pattern applies to external signals, cancellation requests, and Nexus operations when they produce later resolutions.

## Initialization and Workflow Identity

- Machine mode requires exactly one synchronous `#[init]`.
- Its input determines the workflow input type.
- `output = Receipt` declares the workflow output type because there is no main routine.
- `#[run]` and async handlers are compile errors.
- `#[init]` runs only for a fresh Workflow Execution chain. Continued runs restore serialized state without repeating initialization.
- The macro generates `Checkout::workflow` as the typed workflow-definition marker.
- Worker registration remains `register_workflow::<Checkout>()`.
- `name = "checkout"` optionally overrides the default workflow type name.

## Update Semantics

Both styles use `#[update]`; the return type chooses the lifecycle.

For `MachineUpdateResult<T>`:

- The validator, if present, runs first.
- `Ok(value)` accepts and completes the Update synchronously.
- `Err(error)` returns an Update failure without terminating the Workflow.
- The handler may mutate state and stage commands.

For `MachineUpdateResult<DeferredUpdate<T>>`:

- `ctx.defer_update()` creates a deferred response and serializable `UpdateRef<T>`.
- Returning it accepts but does not complete the Update.
- A later handler calls `complete_update` or `fail_update`.
- An error returned before the deferred value rejects the Update.
- Unknown, completed, or type-mismatched references fail the Workflow Task.
- The generated client Update definition exposes output `T`, not `DeferredUpdate<T>`.

Existing `#[update_validator]` syntax and read-only semantics remain unchanged.

## Runtime and Continue-as-New Semantics

- `MachineContext` stages commands and commits them after successful synchronous dispatch.
- Existing typed definitions, options, payload conversion, and failure types are reused; futures are replaced with serializable references.
- User state and durable routing metadata are encoded in a private continuation envelope.
- Explicit continuation uses `ctx.continue_as_new(ContinueAsNewOptions)` and automatically supplies current state.
- Suggested continuation occurs after all mutating jobs in the activation are dispatched.
- Patch and random-seed jobs remain internal, queries remain separate read-only calls, and eviction invokes no handler.
- Custom cancellation tokens are replaced by explicit reference cancellation.
- Local activities gate continuation until a carryover model exists.

## Demo and Test Plan

- Exercise activity, child, and timer starts with explicit typed result handlers.
- Verify timer firing and cancellation route the correct `TimerId` and `TimerResult`.
- Verify duplicate active timer IDs are rejected and resolved IDs can be reused.
- Exercise child start, start failure, execution failure, and completion events.
- Exercise synchronous Update completion and deferred Update acceptance, success, and failure.
- Demonstrate explicit and suggested continue-as-new with outstanding activity, child, timer, and Update references.
- Verify restored runs skip `#[init]` and retain correct handler routing.
- Add compile-fail cases for missing or mismatched handlers, `#[run]`, async handlers, invalid initialization, missing output type, nonserializable state, and invalid deferred Update signatures.
- Keep the imperative Workflow API unchanged.

Assume a future Core/server contract supplies stable identities for carried operations. Until a given operation type supports carryover, automatic continuation is gated while it is outstanding and explicit continuation returns a typed error.
