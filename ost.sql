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
        inner join {{ source('source','status') }} as status on newProduct_leads.macro_id = status.macro_id
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
        min(proposal.attributes) as new_attributes,
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
        coalesce(docs.data->>'customerInterested' != 'no', true) as csi_interested
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
    */
    select distinct on (delivery_stage.stage_id)
        delivery_stage.stage_id as delivery_stage_id,
        formatting(delivery_stage.created_at) as delivery_creation_date,
        newProduct_leads.sales_stage_id,
        newProduct_leads.sales_creation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'schedule') as new_schedule_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'validation') as new_validation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'completion') as new_completion_date,
        min(approvals.doc_approved_date) filter (where docs.name = 'Termination') as new_termination_date
    from newProduct_leads
        inner join {{ source('source','stages') }} as delivery_stage
            on delivery_stage.macro_id = newProduct_leads.macro_id 
            and delivery_stage.type = 'installationChange'
            and newProduct_leads.sales_creation_date < formatting(delivery_stage.created_at)
        left join {{ source('source','docs') }} as docs
            on docs.stage_id = delivery_stage.stage_id 
        left join approvals on approvals.doc_id = docs.doc_id
    where docs.doc_type in ('schedule', 'validation', 'completion', 'custom')
    group by 1, 2, 3, 4
    order by delivery_stage.stage_id, newProduct_leads.sales_creation_date desc
),

system_change_design as (
    select 
        newProduct_leads.sales_stage_id,
        round((design.data->>'systemCapacityKW')::numeric*1000,2) as system_size_pdesign
    from newProduct_leads
        inner join {{ source('source','docs') }} as design
            on design.stage_id = newProduct_leads.sales_stage_id 
            and design.doc_type in ('projectDesign')
),
 
pre_kw as (
    select distinct on (installation_history.project_id)
        installation_history.project_id,
        installation_history.contract_id as prev_contract_id,
        installation_history.attributes as prev_size_watts
    from newProduct_leads 
        inner join {{ source('source','installation_history') }} as installation_history 
            on newProduct_leads.project_id = installation_history.project_id
    where installation_history.created_at < newProduct_leads.sales_creation_date
    order by installation_history.project_id, installation_history.created_at desc
)

/*There are cases when a multiple installationChanges are linked to the same systemChange due to a bad
process follow up. For this reason we add a distinct on, so we report only the first installationChange created
*/
select distinct on (newProduct_leads.sales_stage_id)
    newProduct_leads.main_id,
    newProduct_leads.project_id,
    newProduct_leads.sales_stage_id,
    'https://ops.thinkbright.mx/containers/' || newProduct_leads.macro_id || '/checklists/' || newProduct_leads.sales_stage_id as ops_link,
    newProduct_leads.customer_id,
    (container.attrs->>'csi/assigned_to')::bigint as csi_atribute_ee_id,
    newProduct_leads.sales_creation_date,
    sales_milestones.new_cReport_date,
    sales_milestones.new_signing_date,
    coalesce(delivery_milestones.new_schedule_date, sales_milestones.new_schedule_date) as new_schedule_date,
    (mvisit.start_time)::date as csi_inst_date,
    coalesce(delivery_milestones.new_validation_date, sales_milestones.new_validation_date) as new_validation_date,
    coalesce(delivery_milestones.new_completion_date, sales_milestones.new_completion_date) as new_completion_date,
    case
        when not coalesce(sales_milestones.csi_interested, true) 
        then sales_milestones.new_cReport_date
        else null
    end as csi_not_interested_date,
    delivery_milestones.new_termination_date,
    sales_milestones.new_proposal_id,
    pre_kw.prev_size_watts,
    --Design and proposal system size may have differences between them. We decided to use the design size
    --as the source for measuring the increment since it is what will be physically installed
    sales_milestones.new_attributes as contract_new_attributes,
    system_change_design.system_size_pdesign as design_new_attributes,
    system_change_design.system_size_pdesign - pre_kw.prev_size_watts as increment_watts,
    pre_kw.prev_contract_id,
    contract.contract_id as csi_contract_id,
    coalesce(contract.active, false) as csi_contract_active,
    sales_milestones.new_cReport_current_status,
    coalesce(sales_milestones.csi_interested, true) as csi_interested,
    delivery_milestones.new_termination_date is not null as after_csi_wc_td
from newProduct_leads
    left join sales_milestones on sales_milestones.sales_stage_id = newProduct_leads.sales_stage_id
    left join delivery_milestones on delivery_milestones.sales_stage_id = newProduct_leads.sales_stage_id
    left join pre_kw using (installation_id)
    left join {{ source('source','contract') }} as contract on contract.sheets_proposal_id = sales_milestones.new_proposal_id
        and contract.macro_id = newProduct_leads.macro_id
    left join system_change_design on system_change_design.sales_stage_id = newProduct_leads.sales_stage_id
    left join {{ source('source','container') }} as container on container.macro_id = newProduct_leads.macro_id
    left join {{ ref('metabase_visit') }} as mvisit on mvisit.main_id = newProduct_leads.main_id
        and mvisit.services like '%.installation.%'
        and mvisit.start_time > sales_milestones.new_signing_date
        and (coalesce(sales_milestones.new_validation_date, delivery_milestones.new_validation_date) is null
        or mvisit.start_time < coalesce(sales_milestones.new_validation_date, delivery_milestones.new_validation_date))
order by newProduct_leads.sales_stage_id, delivery_creation_date asc, mvisit.start_time asc
