#include "transaction.h"

void Transaction::add_write_set(Row_occ *row)
{
    if (write_set_ == nullptr)
    {
        write_set_ = new f_set_ent();
        write_set_->tn = txn_id_;
        write_set_->txn = this;
        write_set_->set_size = 1;
        write_set_->rows = new Row_occ *[1];
        write_set_->rows[0] = row;
    }
    else
    {
        f_set_ent *new_write_set = new f_set_ent();
        new_write_set->tn = txn_id_;
        new_write_set->txn = this;
        new_write_set->set_size = write_set_->set_size + 1;
        new_write_set->rows = new Row_occ *[new_write_set->set_size];
        for (uint32_t i = 0; i < write_set_->set_size; i++)
        {
            new_write_set->rows[i] = write_set_->rows[i];
        }
        new_write_set->rows[write_set_->set_size] = row;
        delete write_set_;
        write_set_ = new_write_set;
    }
}

void Transaction::add_read_set(Row_occ *row)
{
    if (read_set_ == nullptr)
    {
        read_set_ = new f_set_ent();
        read_set_->tn = txn_id_;
        read_set_->txn = this;
        read_set_->set_size = 1;
        read_set_->rows = new Row_occ *[1];
        read_set_->rows[0] = row;
    }
    else
    {
        f_set_ent *new_read_set = new f_set_ent();
        new_read_set->tn = txn_id_;
        new_read_set->txn = this;
        new_read_set->set_size = read_set_->set_size + 1;
        new_read_set->rows = new Row_occ *[new_read_set->set_size];
        for (uint32_t i = 0; i < read_set_->set_size; i++)
        {
            new_read_set->rows[i] = read_set_->rows[i];
        }
        new_read_set->rows[read_set_->set_size] = row;
        delete read_set_;
        read_set_ = new_read_set;
    }
}