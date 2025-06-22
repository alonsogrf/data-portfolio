/*
New Product OST: This table is a One Source of Truth for tracking the pipeline at different milestones for the Sales Cycle of this New Product.
 • This New Product id offered in Post Sales stages only for applicable customers.
 • A single customer may have multiple of these new products, implying each product sold has its own sales cycle in the timeline. I want to show all of them.
 • The Sales Cycle is broken down into two stages: 
    1. Sales Stage starting at the call report when the need is detected.
    2. Delivery Stage ending when everything is completed and in production.
        * My project (pending approval, due to priorities) is the designing a unique 'New Product Sales Cycle' Stage that covers both sales and delivery procecess:
            1. Avoiding the tricky part of tying a Sales Stage to its Delivery Stage for multi-products customers, knowing there is no key to match one to the another,
            2. And achieving a streamlined way to track accurate metrics from the sales cycle.
 • In addition, I will explain some workarounds I have taken to clean this up since this it is a process that started as a solution for specific needs of out customers,
   meaning an unstandardized process with solves-the-need steps thas has evolved over the time.
*/

-- List: Narrowed down the dataset by selecting only leads on the stage of interest.
-- This filtering significantly reduced query time by avoiding unnecessary processing of irrelevant records.
with list as (
    select 
        stage.macro_id,
        stage.stage_id as sales_stage_id,
        -- Formatting is a custom function that turns the specific "instant in time" to our local time.
        formatting(stage.created_at) as sales_creation_date
    from {{ source('source','stages') }} as stages
    where stage.type = 'postSales'
),

/*
 • New Product Leads: Allow me to work with a specific list of foreing keys to call all the data from relevant tables for only customers of interest.
    > Heads Up! This list may have multiple duplicated records - as many as the multiple products each customer may have.
    > The unique ID to identify each sales cycle will be sales_stage_id from list CTE.
*/
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

/*
 • The sales cycle of this new product has 6 milestones distribuided on the Sales and Delivery stages:
    1. Call Report: when customer express interest in this product.
    2. Signing: when the product is approved for the customer.
    3. Scheduling: when the team sets in calendar the delivery.
    4. Delivery: the actual delivery of the product to the customer.
    5. Validation: a checklist in order to acomplish a quality level.
    6. Completion: the last process to end this sales cycle.
 • Due to unstardandization I talked before, milestones 3°, 5° and 6° could have been safe on eather the Sales or Delivery stages, even in both of them for worst scenarios.
 • To avoid duplicated info and starting a standard, the link between stages is defined by three rules:
    1. The key to match stages is the stage_creation_date: the last Delivery Stage created goes with the last Sales Stage created before the creation of the current delivery stage.
        * Since the 4° milestone (delivery) is extracted from a distinct table, the logic remains the same: delivery dates between this period goes with this sales cycle.
    2. Milestones 3°, 5° and 6° are prioritized from Delivery Stage. Only when missing, will they be sought in Sales Stage documentation.
        * Milestones 1° and 2° only exists in Sales Stage.
    3. Each milestone is achieved when has its first approval. 
        * Meaning a bad management (backs and forths with customer, implying re-submission of documents for new approvals) will hit the next SLA on the timeline. 
 
  • This path solves for all the scenarios I need:
    a. Deprecate bad follow up procecess: If there is a sales cycle with multiple sales stages or multiple delivery stages, the etablish logic throws a unique way to track it.
    b. Visibility on both closed and open cycles: If there is a new process ongoing (only sales stage without a delivery stage created), I could see it too in this single OST.
    c. Milestones benchmarking: The historical data can be used to uncover patterns and etablishing new goals for a leaner process.
*/

-- Approvals: For only calling the approved milestones
approvals as (
    select
        status.doc_id,
        min(formatting(status.created_at)) as doc_approved_date
    from newProduct_leads
        inner join {{ source('source','status') }} as status on status.macro_id = newProduct_leads.macro_id
    where type = 'document_approval'
    group by 1
),

-- Sales Milestones: Looking for the 1° and 2° milestone. Exploring for the 3°, 5° and 6° milestones, just in case they were documented in this stage.
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
        left join {{ source('source','proposal') }} as proposal on proposal.sheets_proposal_id = trim(docs.data->>'ProposalId')
    where docs.doc_type in ('callReport', 'signing', 'schedule', 'validation', 'completion')
    group by 1, 10
),
 
-- Delivery Milestones: Looking for the 3°, 5° and 6° milestones. Also including a termination milestone for informative purposes only.
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

-- Original Attributes: To set the point of comparison at assesing the change in customer needs.
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

-- Design Attributes: Calling out the new attributes to calculate the change in customer needs.
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
    left join previous_attributes using (installation_id)
    left join {{ source('source','contract') }} as contract on contract.sheets_proposal_id = sales_milestones.new_proposal_id
        and contract.macro_id = newProduct_leads.macro_id
    left join design_attributes on design_attributes.sales_stage_id = newProduct_leads.sales_stage_id
    left join {{ source('source','macros') }} as macros on macros.macro_id = newProduct_leads.macro_id
    left join {{ ref('metabase_visit') }} as deliveries on deliveries.main_id = newProduct_leads.main_id
        and deliveries.services like '%.installation.%'
        and deliveries.start_time > sales_milestones.new_signing_date
        and (coalesce(sales_milestones.new_validation_date, delivery_milestones.new_validation_date) is null
        or deliveries.start_time < coalesce(sales_milestones.new_validation_date, delivery_milestones.new_validation_date))
order by newProduct_leads.sales_stage_id, delivery_creation_date asc, deliveries.start_time asc
