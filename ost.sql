-- This query is inspired by real challenges I have solved in my previous role. 
-- ⚠️ All have been anonymized to ensure confidentiality.

----- NEW PRODUCT OST -----
-- • This dataset acts as a One Source of Truth to track each sales cycle of a new post-sales product offering.
--    > Customers may acquire this product after their initial contract, generating a new sales-delivery flow per product.
--    > Each product cycle is structured in two stages:
--        1. Sales Stage: from interest detection to contract approval.
--        2. Delivery Stage: from scheduling to fulfillment and validation.
--    > Since there's no unique identifier linking both stages (especially when customers have multiple products), 
--      this table establishes a logical connection based on timing rules.
--    > It also helps to normalize a historically unstructured process, enabling performance benchmarking and operational insights.
 
--List: Sales stage filtering, reduces processing by limiting the dataset to Sales Stages related to the post-sales product.
-- • This sets the base for the entire sales cycle timeline.
with list as (
    select 
        stage.macro_id,
        stage.stage_id as sales_stage_id,
        -- Formatting converts the date to local time using a custom function
        formatting(stage.created_at) as sales_creation_date
    from {{ source('source','stages') }} as stages
    where stage.type = 'postSales'
),

-- New Product Leads: Filters only customers linked to the product of interest, extracting relevant IDs.
-- • Heads-up: one customer may appear multiple times if they’ve acquired the product more than once.
-- • The unique identifier for each product cycle is `sales_stage_id`.
newProduct_leads as ( 
    select
        main.main_id,
        main.project_id,
        main.customer_id,
        list.*
    from list
        inner join {{ source('source','main') }} as main on main.macro_id = list.macro_id
        inner join {{ source('source','docs') }} as docs on docs.stage_id = list.sales_stage_id
    where docs.doc_type = 'documentation' 
        and docs.data->>'type' = 'newProduct'
),

----- SALES CYCLE MILESTONE DOCUMENTATION -----
-- • The sales cycle of this product consists of six milestones, distributed across the two key stages:
--    1. Call Report – Captures the moment a customer expresses interest in the product.
--    2. Signing – Marks the formal approval of the product for the customer.
--    3. Scheduling – Represents when the delivery is scheduled on the calendar.
--    4. Delivery – Actual handover of the product to the customer.
--    5. Validation – Quality assurance step, verified via a checklist.
--    6. Completion – Final step to formally close the sales cycle.
-- • Due to prior unstandardized record-keeping, milestones 3 (Scheduling), 5 (Validation), and 6 (Completion)  
--   were inconsistently documented across Sales and Delivery stages, often leading to duplication.

----- SALES CYCLE LINKING RULES -----
-- • To standardize this logic and prevent duplicated records, I apply the following linking rules between Sales and Delivery stages:
--    1. Stage Matching by Creation Date: Each Delivery Stage is linked to the most recent Sales Stage created before it.
--        > Logic extended for the 4th milestone (Delivery) which is pulled in a different stage: 
--          all deliveries within the date range between Sales and Delivery stages are matched accordingly.
--    2. Milestone Source Priority: Milestones 3, 5, and 6 are first sought in the Delivery Stage. If not found, we fall back to the Sales Stage.
--        > Milestones 1 and 2 only exist in the Sales Stage.
--    3. Milestone Achievement Criteria: A milestone is marked as achieved upon its first approval.
--        > Re-submissions due to poor process management will affect SLA measurement by pushing the next milestone's expected timeline.

----- WHY THIS APPROACH WORKS -----
-- • Handles Process Inconsistencies: Even if a sales cycle has multiple Sales or Delivery stages, the logic ensures a single, reliable record for tracking.
-- • Covers All Lifecycle States: Ongoing cycles (Sales stage created but Delivery not yet) are also visible in this unified OST logic.
-- • Supports Continuous Improvement: Clean historical data allows for milestone benchmarking and identifying opportunities to streamline the process.

-- Approvals: Captures the approval timestamp for documents, used to mark when milestones are reached.
-- • Only the first approval per document is considered.
approvals as (
    select
        status.doc_id,
        min(formatting(status.created_at)) as doc_approved_date
    from newProduct_leads
        inner join {{ source('source','status') }} as status on status.macro_id = newProduct_leads.macro_id
    where type = 'document_approval'
    group by 1
),

-- Sales Milestones: Gathers all the document-based milestones available within the Sales Stage.
sales_milestones as (
    select
        newProduct_leads.sales_stage_id,
        -- Always expects a single ProposalId
        string_agg(trim(docs.data->>'ProposalId'), ', ') as new_proposal_id,
        -- In case there’s more than one proposal, retrieve the one with the smallest attributes — manager requirement.
        min(proposal.attributes) as new_proposal_attributes,
        min(docs.status) filter (where docs.doc_type = 'callReport') as new_cReport_current_status,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'callReport') as new_cReport_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'signing') as new_signing_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'schedule') as new_schedule_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'validation') as new_validation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'completion') as new_completion_date,
        ----- DEALING WITH INCOMPLETE DOCUMENTATION -----
        -- Assumption:
        --    a. A 'no' is always explicitly documented when the customer is not interested.
        --    b. A 'yes' may be left undocumented if the team moves forward with the process.
        -- Therefore, any blank entry is treated as an implicit 'yes' (customer is interested).
        coalesce(docs.data->>'customerInterested' != 'no', true) as new_has_interest
    from newProduct_leads
        inner join {{ source('source','docs') }} as docs on docs.stage_id = newProduct_leads.sales_stage_id
        inner join approvals on approvals.doc_id = docs.doc_id
        left join {{ source('source','proposal') }} as proposal on proposal.proposal_id = trim(docs.data->>'ProposalId')
    where docs.doc_type in ('callReport', 'signing', 'schedule', 'validation', 'completion')
    group by 1, 10
),
 
-- Delivery Milestones: Retrieves later-stage milestones from Delivery Stage documents. 
-- • Includes a termination milestone for informational purposes only.
delivery_milestones as (
--   • As mentioned earlier, the `macro_id` in the `newProduct_leads` table may appear multiple times:
--      > Each `macro_id` represents a customer and repeats for every product they have, identified by its unique `sales_stage_id`.
--   • That's why this `distinct on` is used: to ensure only one row per `delivery_stage_id` is shown.
--      > Instead of one row per sales_stage_id × delivery_stage_id pair
--   • The `order by` clause ensures that the most recent `sales_stage_id` (created before the `delivery_stage_id`)
--     is the one linked to that unique `delivery_stage_id`.
    select distinct on (delivery_stage.stage_id)
        delivery_stage.stage_id as delivery_stage_id,
        formatting(delivery_stage.created_at) as delivery_creation_date,
        newProduct_leads.sales_stage_id,
        newProduct_leads.sales_creation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'schedule') as new_schedule_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'validation') as new_validation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'completion') as new_completion_date,
        -- • This is a newly introduced process, so there is no predefined document for the anticipated termination of this specific contract in our CRM.
        -- • As a workaround, the team is currently creating custom entries on a case-by-case basis to fulfill the documentation need.
        min(approvals.doc_approved_date) filter (where docs.name = 'Termination') as new_termination_date
    from newProduct_leads
        inner join {{ source('source','stages') }} as delivery_stage on delivery_stage.macro_id = newProduct_leads.macro_id 
            and delivery_stage.type = 'deliveryStage'
            and newProduct_leads.sales_creation_date < formatting(delivery_stage.created_at)
        left join {{ source('source','docs') }} as docs on docs.stage_id = delivery_stage.stage_id 
        inner join approvals on approvals.doc_id = docs.doc_id
    where docs.doc_type in ('schedule', 'validation', 'completion', 'custom')
    group by 1, 2, 3, 4
    order by delivery_stage.stage_id, newProduct_leads.sales_creation_date desc
),

-- • For analytical purposes, this section aims to identify trends in the types of customers acquiring this new product.
--    > The full analysis includes attributes of the acquired products, highlighting common patterns in customer new needs.
-- • However, to assess these 'incremental needs', discrepancies must be considered:
--    > Due to outdated attributes at the time contracts are signed, there may be differences between the attributes in the proposal and those in the final delivered product.
--        * This does not affect the customer, as all contractual needs are met through the final design using the current product catalog.
-- • Following my manager’s recommendation, both sets of attributes are included for reference:
--    > `new_proposal_attributes` (from `sales_milestones`)
--    > `design_attributes` (constructed below)
-- • However, only `design_attributes` will be used to measure the evolving customer needs, as it reflects the actual delivered specifications.

-- Previous Attributes: Captures the original specs before the new product was acquired. 
previous_attributes as (
    -- • `distinct on` is needed because Project History logs an entry each time the billing status changes. I want to return a single record per `project_id`.
    select distinct on (project_history.project_id)
        project_history.project_id,
        project_history.contract_id as previous_contract_id,
        project_history.attributes as original_attributes
    from newProduct_leads 
        inner join {{ source('source','project_history') }} as project_history on project_history.project_id = newProduct_leads.project_id
    where formatting(project_history.created_at) < newProduct_leads.sales_creation_date
    order by project_history.project_id, project_history.created_at desc
),

-- Design Attributes: Final product specs after delivery, used to assess how much customer needs evolved.
design_attributes as (
    select 
        newProduct_leads.sales_stage_id,
        -- Numeric processing required to establish the same units as of the `original_attributes` from `previous_attributes` CTE.
        round((attributes.data->>'someAttributes')::numeric*10000,2) as new_design_attributes
    from newProduct_leads
        inner join {{ source('source','docs') }} as attributes on attributes.stage_id = newProduct_leads.sales_stage_id
            and design.doc_type in ('designAttributes')
)

-- Main Table: Built based on the declared principles:
--    > Use `distinct on` to report a single sales cycle per product: one sales stage matched with one delivery stage.
--    > Each product is shown as a separate record, resulting in multiple entries per customer.
--    > Milestones are primarily retrieved from the delivery stage. If unavailable, fallback to the sales stage.
--    > The relationship between sales and delivery stages is determined by their creation dates. Deliveries scheduled in this range are linked to the product cycle.
--    > Customer needs evolution is measured by comparing the original product attributes with the final design — not the signed contract specs.
select distinct on (newProduct_leads.sales_stage_id)
    newProduct_leads.main_id,
    newProduct_leads.project_id,
    newProduct_leads.sales_stage_id,
    delivery_milestones.delivery_stage_id,
    'https://CRM/...' || ... || '...' || ... as crm_url,
    newProduct_leads.customer_id,
    (macros.attributes->>'salesOwner')::bigint as sales_owner_id,
    newProduct_leads.sales_creation_date,
    sales_milestones.new_cReport_date,
    sales_milestones.new_signing_date,
    coalesce(delivery_milestones.new_schedule_date, sales_milestones.new_schedule_date) as new_schedule_date,
    (deliveries.start_time)::date as new_delivery_date,
    coalesce(delivery_milestones.new_validation_date, sales_milestones.new_validation_date) as new_validation_date,
    coalesce(delivery_milestones.new_completion_date, sales_milestones.new_completion_date) as new_completion_date,
    case
        when not coalesce(sales_milestones.new_has_interest, true) 
        then sales_milestones.new_cReport_date
        else null
    end as new_not_interested_date,
    delivery_milestones.new_termination_date,
    sales_milestones.new_proposal_id,
    previous_attributes.original_attributes,
    sales_milestones.new_proposal_attributes,
    design_attributes.new_design_attributes,
    design_attributes.new_design_attributes - previous_attributes.original_attributes as incremental_needs,
    previous_attributes.previous_contract_id,
    contract.contract_id as new_contract_id,
    coalesce(contract.active, false) as new_contract_active,
    sales_milestones.new_cReport_current_status,
    coalesce(sales_milestones.new_has_interest, true) as new_has_interest,
    delivery_milestones.new_termination_date is not null as termination_requested,
from newProduct_leads
    left join sales_milestones on sales_milestones.sales_stage_id = newProduct_leads.sales_stage_id
    left join delivery_milestones on delivery_milestones.sales_stage_id = newProduct_leads.sales_stage_id
    left join previous_attributes using (project_id)
    left join {{ source('source','contract') }} as contract on contract.proposal_id = sales_milestones.new_proposal_id
        and contract.macro_id = newProduct_leads.macro_id
    left join design_attributes on design_attributes.sales_stage_id = newProduct_leads.sales_stage_id
    left join {{ source('source','macros') }} as macros on macros.macro_id = newProduct_leads.macro_id
    left join {{ ref('deliveries') }} as deliveries on deliveries.main_id = newProduct_leads.main_id
        and deliveries.services like '%.project.%'
        and deliveries.start_time > sales_milestones.new_signing_date
        and (coalesce(delivery_milestones.new_validation_date, sales_milestones.new_validation_date) is null
        or deliveries.start_time < coalesce(delivery_milestones.new_validation_date, sales_milestones.new_validation_date))
order by newProduct_leads.sales_stage_id, delivery_creation_date asc, deliveries.start_time asc

----- NEXT STEPS -----
-- My proposed project (pending approval due to prioritization) involves designing a unified New Product Sales Cycle Stage 
-- that integrates both Sales and Delivery processes, aiming to:
--    a. Eliminate the complexity of linking Sales and Delivery Stages for multi-product customers, given there's no 
--       reliable key to match them.
--    b. Enable a streamlined, consistent framework for tracking accurate metrics across the full sales cycle.
