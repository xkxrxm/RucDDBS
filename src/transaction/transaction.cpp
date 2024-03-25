#include "transaction.h"

void Transaction::add_write_set(Row_occ *row)
{
    auto new_rows = new Row_occ *[write_set_->set_size + 1];
    for (uint32_t i = 0; i < write_set_->set_size; i++)
    {
        new_rows[i] = write_set_->rows[i];
        }
        new_rows[write_set_->set_size] = row;
        auto ptr = write_set_->rows;
        write_set_->rows = new_rows;
        write_set_->set_size += 1;
        delete[] ptr;
}

void Transaction::add_read_set(Row_occ *row)
{
    auto new_rows = new Row_occ *[read_set_->set_size + 1];
    for (uint32_t i = 0; i < read_set_->set_size; i++)
    {
        new_rows[i] = read_set_->rows[i];
        }
        new_rows[read_set_->set_size] = row;
        auto ptr = read_set_->rows;
        read_set_->rows = new_rows;
        read_set_->set_size += 1;
        delete[] ptr;
}