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

-- New Product Leads: Allow me to work with a specific list of foreing keys to call all the data from relevant tables for only customers of interest.
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

-- HERE
installation_change_dates as (
    /*The CSI process includes two checklists: System Change and Installation Change. However, a single lead
    may have multiple, incomplete CSI processes.
    To link correctly the checklist, we follow this rule: the Installation Change should be linked to the 
    most recent System Change checklist created before the Installation Change.
    */
    select distinct on (installation_change_stage.stage_id)
        installation_change_stage.stage_id as instal_change_checklist_id,
        formatting(installation_change_stage.created_at) as instal_change_creation_date,
        newProduct_leads.sales_stage_id,
        newProduct_leads.sales_creation_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'welcomeCall') as csi_wc_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'installationFunctionDemo') as csi_com_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'interconnection') as csi_ix_date,
        min(approvals.doc_approved_date) filter (where docs.name = 'After WC CSI TD') as csi_td_date
    from newProduct_leads
        inner join {{ source('source','stages') }} as installation_change_checklist
            on installation_change_stage.macro_id = newProduct_leads.macro_id 
            and installation_change_stage.type = 'installationChange'
            and newProduct_leads.sales_creation_date < formatting(installation_change_stage.created_at)
        left join {{ source('source','checklist_item') }} as checklist_item
            on docs.stage_id = installation_change_stage.stage_id 
        left join approvals on approvals.doc_id = docs.doc_id
    where docs.doc_type in ('welcomeCall', 'interconnection', 'installationFunctionDemo', 'custom')
    group by 1, 2, 3, 4
    order by installation_change_stage.stage_id, newProduct_leads.sales_creation_date desc
),

/* Due to multiple changes on CSI process. The Welcome Call, Commissioning, and IX checklist_items
could be contained either on a systemChange or an installationChange checklist or even in both of them
causing some duplication.
We’ve decided to get the date of the first approval for each type of item from both checklists. And then 
prioritize the dates from installationChange when available.
*/
system_change_dates as (
    select
        newProduct_leads.sales_stage_id,
        --we always expects only 1 sheetsProposalId
        string_agg(trim(docs.data->>'sheetsProposalId'), ', ') as csi_proposal_id,
        min(proposal.system_size_watts) as new_size_watts,
        min(docs.status) filter (where docs.doc_type = 'systemChangeCustomerCallReport') as csi_rsv_current_status,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'systemChangeCustomerCallReport') as csi_rsv_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'subscriptionContract') as csi_app_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'welcomeCall') as csi_wc_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'installationFunctionDemo') as csi_com_date,
        min(approvals.doc_approved_date) filter (where docs.doc_type = 'interconnection') as csi_ix_date,
        --here we assume the interest is true if the space is empty
        coalesce(docs.data->>'customerInterested' != 'no', true) as csi_interested
    from newProduct_leads
        inner join {{ source('source','checklist_item') }} as checklist_item on docs.stage_id = newProduct_leads.sales_stage_id
        inner join approvals on approvals.doc_id = docs.doc_id
        left join {{ source('source','proposal') }} as proposal on proposal.sheets_proposal_id = trim(docs.data->>'sheetsProposalId')
    where docs.doc_type in ('systemChangeCustomerCallReport', 'subscriptionContract', 'welcomeCall', 'interconnection', 'installationFunctionDemo')
    group by 1, 10
),

system_change_design as (
    select 
        newProduct_leads.sales_stage_id,
        round((design.data->>'systemCapacityKW')::numeric*1000,2) as system_size_pdesign
    from newProduct_leads
        inner join {{ source('source','checklist_item') }} as design
            on design.stage_id = newProduct_leads.sales_stage_id 
            and design.doc_type in ('projectDesign')
),
 
pre_kw as (
    select distinct on (installation_history.project_id)
        installation_history.project_id,
        installation_history.contract_id as prev_contract_id,
        installation_history.system_size_watts as prev_size_watts
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
    system_change_dates.csi_rsv_date,
    system_change_dates.csi_app_date,
    coalesce(installation_change_dates.csi_wc_date, system_change_dates.csi_wc_date) as csi_wc_date,
    (mvisit.start_time)::date as csi_inst_date,
    coalesce(installation_change_dates.csi_com_date, system_change_dates.csi_com_date) as csi_com_date,
    coalesce(installation_change_dates.csi_ix_date, system_change_dates.csi_ix_date) as csi_ix_date,
    case
        when not coalesce(system_change_dates.csi_interested, true) 
        then system_change_dates.csi_rsv_date
        else null
    end as csi_not_interested_date,
    installation_change_dates.csi_td_date,
    system_change_dates.csi_proposal_id,
    pre_kw.prev_size_watts,
    --Design and proposal system size may have differences between them. We decided to use the design size
    --as the source for measuring the increment since it is what will be physically installed
    system_change_dates.new_size_watts as contract_new_size_watts,
    system_change_design.system_size_pdesign as design_new_size_watts,
    system_change_design.system_size_pdesign - pre_kw.prev_size_watts as increment_watts,
    pre_kw.prev_contract_id,
    contract.contract_id as csi_contract_id,
    coalesce(contract.active, false) as csi_contract_active,
    system_change_dates.csi_rsv_current_status,
    coalesce(system_change_dates.csi_interested, true) as csi_interested,
    installation_change_dates.csi_td_date is not null as after_csi_wc_td
from newProduct_leads
    left join system_change_dates on system_change_dates.sales_stage_id = newProduct_leads.sales_stage_id
    left join installation_change_dates on installation_change_dates.sales_stage_id = newProduct_leads.sales_stage_id
    left join pre_kw using (installation_id)
    left join {{ source('source','contract') }} as contract on contract.sheets_proposal_id = system_change_dates.csi_proposal_id
        and contract.macro_id = newProduct_leads.macro_id
    left join system_change_design on system_change_design.sales_stage_id = newProduct_leads.sales_stage_id
    left join {{ source('source','container') }} as container on container.macro_id = newProduct_leads.macro_id
    left join {{ ref('metabase_visit') }} as mvisit on mvisit.main_id = newProduct_leads.main_id
        and mvisit.services like '%.installation.%'
        and mvisit.start_time > system_change_dates.csi_app_date
        and (coalesce(system_change_dates.csi_com_date, installation_change_dates.csi_com_date) is null
        or mvisit.start_time < coalesce(system_change_dates.csi_com_date, installation_change_dates.csi_com_date))
order by newProduct_leads.sales_stage_id, instal_change_creation_date asc, mvisit.start_time asc
