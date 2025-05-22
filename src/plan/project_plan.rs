use crate::plan::Plan;
use crate::query::Scan;
use crate::record::schema::Schema;
use crate::query::project_scan::ProjectScan;

pub struct ProjectPlan<'tx> {
    plan: Box<dyn Plan<'tx> + 'tx>,
    schema: Schema,
    fieldlist: Vec<String>,
}

impl<'tx> ProjectPlan<'tx> {
    pub fn new(plan: Box<dyn Plan<'tx> + 'tx>, fields: Vec<String>) -> Self {
        let mut schema = Schema::new();
        for field in &fields {
            schema.add_from_schema(field, &plan.schema());
        }
        ProjectPlan { plan, schema, fieldlist: fields }
    }
}

impl<'tx> Plan<'tx> for ProjectPlan<'tx> {
    fn open(&self) -> Box<dyn Scan + 'tx> {
        let scan = self.plan.open();
        Box::new(ProjectScan::new(scan, self.fieldlist.clone()))
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}