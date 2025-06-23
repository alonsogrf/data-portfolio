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
 
-- • List: Reduces processing by limiting the dataset to Sales Stages related to the post-sales product.
-- • This sets the base for the entire sales cycle timeline.
with list as (
    select 
        stage.macro_id,
        stage.stage_id as sales_stage_id,
        -- Formatting is a custom function that turns the specific "instant in time" to our local time.
        formatting(stage.created_at) as sales_creation_date
    from {{ source('source','stages') }} as stages
    where stage.type = 'postSales'
),

-- • New Product Leads: Filters only customers linked to the product of interest, extracting relevant IDs.
--    > Heads-up: one customer may appear multiple times if they’ve acquired the product more than once.
--    > The unique identifier for each product cycle is `sales_stage_id`.
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
-- • Due to prior unstandardized record-keeping, milestones 3 (Scheduling), 5 (Validation), and 6 (Completion) may have 
--   been documented in either or both stages, leading to potential duplication.

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
        -- I always expect only 1 ProposalId
        string_agg(trim(docs.data->>'ProposalId'), ', ') as new_proposal_id,
        -- Just in case there could be more than 1 proposal, I will get the information of the smallest attributes - restriction made from my manager
        min(proposal.attributes) as new_proposal_attributes,
        min(docs.status) filter (where docs.doc_type = 'callReport') as new_cReport_current_status,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'callReport') as new_cReport_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'signing') as new_signing_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'schedule') as new_schedule_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'validation') as new_validation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'completion') as new_completion_date,
        /*
        Here I am dealing with incomplete documentation. I assume:
              a. There is a 100% probability whenever a customer is not interested, the analysts would document the case and mark a 'no' option.
              b. Whenever we have an interested customer, the team could hop to another part of the process, forgetting about documenting the affirmative interest.
        That is why whenever there may be an empty space, I will take it as a reafirmation for customer interest.
        */
        coalesce(docs.data->>'customerInterested' != 'no', true) as new_has_interest
    from newProduct_leads
        inner join {{ source('source','docs') }} as docs on docs.stage_id = newProduct_leads.sales_stage_id
        inner join approvals on approvals.doc_id = docs.doc_id
        left join {{ source('source','proposal') }} as proposal on proposal.proposal_id = trim(docs.data->>'ProposalId')
    where docs.doc_type in ('callReport', 'signing', 'schedule', 'validation', 'completion')
    group by 1, 10
),
 
-- Delivery Milestones: Retrieves later-stage milestones from Delivery Stage documents. 
-- • Also including a termination milestone for informative purposes only.
delivery_milestones as (
    /*
     • As I said before, the macro_id from newProduct_leads table may contains duplicated values:
         > One macro_id per customer, displaying as many rows as products the customer have - identifiyng each product by its sales_stage_id
     • That is the reason behind this distinct on: I will display 1 row for each delivery stage (instead of 1 row for each combination of sales_stage_id vs delivery_stage_id)
         > The Order By is in charge of linking the last sales_stage_id (before de creation of the delivery_stage_id) only to its unique delivery_stage_id
    */
    select distinct on (delivery_stage.stage_id)
        delivery_stage.stage_id as delivery_stage_id,
        formatting(delivery_stage.created_at) as delivery_creation_date,
        newProduct_leads.sales_stage_id,
        newProduct_leads.sales_creation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'schedule') as new_schedule_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'validation') as new_validation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'completion') as new_completion_date,
        -- Since this is a new process, the document for the anticipated termination of this specific contract does not even exists in our CRM.
        -- The team is currently using this workaround to document a termination: creating custom documents on demand - just for covering the need.
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

/*
 • For analytical purposes, I want to show trends/patterns on the "kind of customers" that adquiare this new product.
    > The complete analysis also includes the attributes on the new products adquired - showing out the incremental needs in where customers use to fall.
 • However, when trying to measure this incremental need I face with some discrepancies that force the analysis to measure the attributes from the design of the project:
    > As a result of outdated attributes by the time of filling the contract, there may be a gap between the attributes at the proposal and attributes from the final product delivered.
        * This does not affect the customer since all their needs (signed in the contract) will be covered in the design - using the current catalog of products.
 • Following an advice from my manager, I decided to include both attributes for reference only: new_proposal_attributes (from sales_milestones) and design_attributes (construction below)
*/

-- Previous Attributes: Captures the original specs before the new product was acquired. 
-- • Useful to measure change in customer needs.
previous_attributes as (
    -- Distinct on needed since the Project History receives one log every time the billing status changes. I am looking for just one record per project_id.
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
        -- Numeric processing required to establish the same units as of the original_attributes from previous_attributes CTE.
        round((attributes.data->>'someAttributes')::numeric*10000,2) as new_design_attributes
    from newProduct_leads
        inner join {{ source('source','docs') }} as attributes on attributes.stage_id = newProduct_leads.sales_stage_id
            and design.doc_type in ('designAttributes')
)

/*
 • Finally, to build the main table I retrive the declared principles:
    > I need a distinct on to only report one sales cycle per product: (almost) each sales stage matching a single delivery stage.
    > This also means that will be displayed each product: there will be multiple records per customer.
    > The applicable milestones are going to be retrived from delivery stage when posible. Only if missing, they would be looked up from sales stage.
    > The relation between sales and delivery stage is build by their creation dates. The deliveries scheduled within this period will be linked to this sales cycle. 
    > The change on customer needs are measured from the comparison between the original product and the designed project (instead of the signed specifications from the contract)
*/
-- Final Output: Returns one row per product cycle with all key milestones and reference attributes.
-- • Prioritizes delivery data over sales when duplicated.
-- • Uses creation date matching logic to link Sales and Delivery stages.
-- • Includes both original and final system specs to support behavioral analytics.
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
